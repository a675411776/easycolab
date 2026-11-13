import Foundation
import Darwin

// MARK: - EasyColab 本机固定端口反向代理网关 (专为 Magpie / Claude Code / 本地开发免改配置设计)

struct LocalGatewaySnapshot {
    var upstreamBaseURL: String = ""
    var apiAccessKey: String = ""
    var isConnected: Bool = false
    var activeModelRepo: String = "Qwen/Qwen3.8-27B"
    var activeModelName: String = "Qwen3.8-27B"
    var customRepoIds: [String] = []
    var contextLengthTokens: Int = 262144
}

final class LocalAPIGatewayServer: NSObject, URLSessionDataDelegate {
    static let virtualModelID = "easycolab-auto"
    static let defaultPort = 8787

    private let stateLock = NSLock()
    private var snapshot = LocalGatewaySnapshot()
    private var listenFD: Int32 = -1
    private var activePort: Int = 0
    private var isRunningFlag: Bool = false

    private let relayLock = NSLock()
    private var relays: [Int: StreamTaskRelay] = [:]

    private lazy var delegateQueue: OperationQueue = {
        let q = OperationQueue()
        q.name = "app.easycolab.localgateway.stream"
        q.maxConcurrentOperationCount = 16
        q.qualityOfService = .userInitiated
        return q
    }()

    private lazy var upstreamSession: URLSession = {
        let cfg = URLSessionConfiguration.ephemeral
        cfg.timeoutIntervalForRequest = 600
        cfg.timeoutIntervalForResource = 3600
        cfg.httpMaximumConnectionsPerHost = 16
        cfg.waitsForConnectivity = false
        cfg.requestCachePolicy = .reloadIgnoringLocalCacheData
        cfg.urlCache = nil
        return URLSession(configuration: cfg, delegate: self, delegateQueue: delegateQueue)
    }()

    var onRequestForwarded: (() -> Void)?

    private static let strippedRequestHeaders: Set<String> = [
        "host",
        "origin",
        "referer",
        "connection",
        "keep-alive",
        "proxy-connection",
        "upgrade",
        "accept-encoding",
        "content-length",
        "transfer-encoding",
        "expect",
        "sec-fetch-site",
        "sec-fetch-mode",
        "sec-fetch-dest",
        "sec-fetch-user"
    ]

    // MARK: - 状态同步与启停控制

    func updateSnapshot(_ next: LocalGatewaySnapshot) {
        stateLock.lock()
        snapshot = next
        stateLock.unlock()
    }

    private func currentSnapshot() -> LocalGatewaySnapshot {
        stateLock.lock()
        defer { stateLock.unlock() }
        return snapshot
    }

    @discardableResult
    func start(port: Int) -> Result<Int, Error> {
        let clampedPort = max(1024, min(65535, port))

        stateLock.lock()
        if isRunningFlag && activePort == clampedPort && listenFD >= 0 {
            stateLock.unlock()
            return .success(clampedPort)
        }
        stateLock.unlock()

        stop()

        let fd = Darwin.socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
        guard fd >= 0 else {
            let code = errno
            return .failure(Self.makeSocketError(code: code, fallback: "无法创建本地监听 Socket"))
        }

        var yes: Int32 = 1
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, socklen_t(MemoryLayout<Int32>.size))
        _ = Darwin.setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, socklen_t(MemoryLayout<Int32>.size))

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(UInt16(clampedPort).bigEndian)
        addr.sin_addr = in_addr(s_addr: UInt32(INADDR_LOOPBACK).bigEndian)

        let bindResult = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        if bindResult != 0 {
            let code = errno
            Darwin.close(fd)
            if code == EADDRINUSE {
                return .failure(NSError(
                    domain: "LocalAPIGateway",
                    code: Int(code),
                    userInfo: [NSLocalizedDescriptionKey: L("端口 \(clampedPort) 已被其他程序占用，请更换端口后重试。", "Port \(clampedPort) is already in use. Try another port.")]
                ))
            }
            return .failure(Self.makeSocketError(code: code, fallback: "绑定 127.0.0.1:\(clampedPort) 失败"))
        }

        if Darwin.listen(fd, 128) != 0 {
            let code = errno
            Darwin.close(fd)
            return .failure(Self.makeSocketError(code: code, fallback: "监听 127.0.0.1:\(clampedPort) 失败"))
        }

        stateLock.lock()
        listenFD = fd
        activePort = clampedPort
        isRunningFlag = true
        stateLock.unlock()

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.acceptLoop(serverFD: fd)
        }

        return .success(clampedPort)
    }

    func stop() {
        stateLock.lock()
        let oldFD = listenFD
        listenFD = -1
        activePort = 0
        isRunningFlag = false
        stateLock.unlock()

        if oldFD >= 0 {
            _ = Darwin.shutdown(oldFD, SHUT_RDWR)
            Darwin.close(oldFD)
        }

        relayLock.lock()
        let activeRelays = Array(relays.values)
        relays.removeAll()
        relayLock.unlock()

        for relay in activeRelays {
            relay.cancelAndClose()
        }
    }

    private static func makeSocketError(code: Int32, fallback: String) -> NSError {
        let msg = String(cString: strerror(code))
        return NSError(
            domain: "LocalAPIGateway",
            code: Int(code),
            userInfo: [NSLocalizedDescriptionKey: "\(fallback) (\(msg))"]
        )
    }

    // MARK: - Socket 接入与 HTTP/1.1 请求解析

    private func acceptLoop(serverFD: Int32) {
        while true {
            var clientAddr = sockaddr_in()
            var addrLen = socklen_t(MemoryLayout<sockaddr_in>.size)
            let clientFD = withUnsafeMutablePointer(to: &clientAddr) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.accept(serverFD, $0, &addrLen)
                }
            }

            if clientFD < 0 {
                let err = errno
                if err == EINTR { continue }
                stateLock.lock()
                let stillSame = (listenFD == serverFD && isRunningFlag)
                stateLock.unlock()
                if !stillSame { break }
                Thread.sleep(forTimeInterval: 0.02)
                continue
            }

            var one: Int32 = 1
            _ = Darwin.setsockopt(clientFD, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
            _ = Darwin.setsockopt(clientFD, IPPROTO_TCP, TCP_NODELAY, &one, socklen_t(MemoryLayout<Int32>.size))
            var ioTimeout = timeval(tv_sec: 30, tv_usec: 0)
            _ = Darwin.setsockopt(clientFD, SOL_SOCKET, SO_RCVTIMEO, &ioTimeout, socklen_t(MemoryLayout<timeval>.size))
            _ = Darwin.setsockopt(clientFD, SOL_SOCKET, SO_SNDTIMEO, &ioTimeout, socklen_t(MemoryLayout<timeval>.size))

            DispatchQueue.global(qos: .userInitiated).async { [weak self] in
                guard let self else {
                    Darwin.close(clientFD)
                    return
                }
                self.handleClientConnection(clientFD: clientFD)
            }
        }
    }

    private func handleClientConnection(clientFD: Int32) {
        let reader = SocketByteReader(fd: clientFD)
        guard let req = reader.readHTTPRequest() else {
            Darwin.close(clientFD)
            return
        }

        if req.method == "OPTIONS" {
            Self.sendSimpleResponse(
                fd: clientFD,
                statusCode: 204,
                statusText: "No Content",
                contentType: "text/plain; charset=utf-8",
                body: Data()
            )
            Darwin.close(clientFD)
            return
        }

        let cleanPath = req.target.components(separatedBy: "?").first ?? req.target
        let snap = currentSnapshot()

        if req.method == "GET" && (cleanPath == "/" || cleanPath == "/health") {
            let portVal = currentPortValue()
            let healthObj: [String: Any] = [
                "status": "ok",
                "proxy_url": "http://127.0.0.1:\(portVal)",
                "openai_base_url": "http://127.0.0.1:\(portVal)/v1",
                "anthropic_base_url": "http://127.0.0.1:\(portVal)",
                "recommended_model": Self.virtualModelID,
                "active_model_name": snap.activeModelName,
                "active_model_repo": snap.activeModelRepo,
                "upstream_connected": snap.isConnected,
                "upstream_configured": !snap.upstreamBaseURL.isEmpty
            ]
            let body = (try? JSONSerialization.data(withJSONObject: healthObj, options: [.prettyPrinted])) ?? Data("{}".utf8)
            Self.sendJSONResponse(fd: clientFD, statusCode: 200, body: body)
            Darwin.close(clientFD)
            return
        }

        if req.method == "GET" && (cleanPath == "/v1/models" || cleanPath == "/models") {
            handleModelsEndpoint(clientFD: clientFD, snapshot: snap)
            return
        }

        forwardToColabUpstream(clientFD: clientFD, request: req, cleanPath: cleanPath, snapshot: snap)
    }

    private func currentPortValue() -> Int {
        stateLock.lock()
        defer { stateLock.unlock() }
        return activePort > 0 ? activePort : Self.defaultPort
    }

    // MARK: - /v1/models（支持离线探测，确保 Magpie 关机状态也可配置与校验通过）

    private func handleModelsEndpoint(clientFD: Int32, snapshot: LocalGatewaySnapshot) {
        let nowTs = Int(Date().timeIntervalSince1970)
        let ctxTokens = max(4096, snapshot.contextLengthTokens)
        let virtualEntry: [String: Any] = [
            "id": Self.virtualModelID,
            "object": "model",
            "created": nowTs,
            "owned_by": "easycolab-local-gateway",
            "loaded": snapshot.isConnected,
            "context_length": ctxTokens,
            "max_input_tokens": ctxTokens
        ]

        func isNonLLMModelID(_ modelID: String) -> Bool {
            let lower = modelID.lowercased()
            return lower.contains("image") || lower.contains("minimax-h3") || lower.contains("video")
        }

        func buildFallbackModelsData() -> Data {
            var list: [[String: Any]] = [virtualEntry]
            var seen: Set<String> = [Self.virtualModelID]
            let candidates = snapshot.customRepoIds + [
                snapshot.activeModelRepo,
                "unsloth/Qwen3.8-27B-GGUF",
                "Qwen/Qwen3.8-27B",
                "Qwen/Qwen-Image-2.1",
                "Tongyi-MAI/Z-Image-Turbo"
            ]
            for repo in candidates where !repo.isEmpty && !seen.contains(repo) {
                seen.insert(repo)
                var item: [String: Any] = [
                    "id": repo,
                    "object": "model",
                    "created": nowTs,
                    "owned_by": "easycolab",
                    "loaded": snapshot.isConnected && repo == snapshot.activeModelRepo
                ]
                if !isNonLLMModelID(repo) {
                    item["context_length"] = ctxTokens
                    item["max_input_tokens"] = ctxTokens
                }
                list.append(item)
            }
            let payload: [String: Any] = ["object": "list", "data": list]
            return (try? JSONSerialization.data(withJSONObject: payload)) ?? Data("{\"object\":\"list\",\"data\":[]}".utf8)
        }

        guard !snapshot.upstreamBaseURL.isEmpty,
              let upstreamURL = URL(string: "\(snapshot.upstreamBaseURL)/v1/models") else {
            Self.sendJSONResponse(fd: clientFD, statusCode: 200, body: buildFallbackModelsData())
            Darwin.close(clientFD)
            return
        }

        var urlReq = URLRequest(url: upstreamURL)
        urlReq.httpMethod = "GET"
        urlReq.timeoutInterval = snapshot.isConnected ? 6.0 : 2.5
        if !snapshot.apiAccessKey.isEmpty {
            urlReq.setValue("Bearer \(snapshot.apiAccessKey)", forHTTPHeaderField: "Authorization")
            urlReq.setValue(snapshot.apiAccessKey, forHTTPHeaderField: "x-api-key")
        }

        let sem = DispatchSemaphore(value: 0)
        var fetchedBody: Data? = nil

        let task = URLSession.shared.dataTask(with: urlReq) { data, resp, err in
            defer { sem.signal() }
            guard err == nil,
                  let http = resp as? HTTPURLResponse,
                  http.statusCode == 200,
                  let data,
                  var obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
                  var items = obj["data"] as? [[String: Any]] else {
                return
            }
            for idx in items.indices {
                if let mid = items[idx]["id"] as? String, !isNonLLMModelID(mid) {
                    if items[idx]["context_length"] == nil {
                        items[idx]["context_length"] = ctxTokens
                    }
                    if items[idx]["max_input_tokens"] == nil {
                        items[idx]["max_input_tokens"] = ctxTokens
                    }
                }
            }
            let hasVirtual = items.contains { ($0["id"] as? String) == Self.virtualModelID }
            if !hasVirtual {
                items.insert(virtualEntry, at: 0)
            }
            obj["data"] = items
            fetchedBody = try? JSONSerialization.data(withJSONObject: obj)
        }
        task.resume()
        _ = sem.wait(timeout: .now() + 6.5)

        Self.sendJSONResponse(fd: clientFD, statusCode: 200, body: fetchedBody ?? buildFallbackModelsData())
        Darwin.close(clientFD)
    }

    // MARK: - 转发推理请求（支持 SSE 零缓冲流式透传、自动注入密钥、断连取消 GPU 推理）

    private func forwardToColabUpstream(
        clientFD: Int32,
        request: ParsedHTTPRequest,
        cleanPath: String,
        snapshot: LocalGatewaySnapshot
    ) {
        let isAnthropic = Self.isAnthropicEndpoint(cleanPath)

        guard !snapshot.upstreamBaseURL.isEmpty,
              let targetURL = URL(string: "\(snapshot.upstreamBaseURL)\(request.target)") else {
            let errData = Self.makeOfflineErrorJSON(
                isAnthropic: isAnthropic,
                reason: L("尚未连接 Colab 云端实例", "Colab instance not connected yet")
            )
            Self.sendJSONResponse(fd: clientFD, statusCode: 503, body: errData)
            Darwin.close(clientFD)
            return
        }

        let finalBody = (request.method == "POST") ? Self.normalizeModelFieldIfEmpty(request.body) : request.body

        var urlReq = URLRequest(url: targetURL)
        urlReq.httpMethod = request.method
        // 未处于已连接心跳时快速失败，便于 Magpie 立即触发 Failover；已连接时给足大上下文/生图超时时间
        urlReq.timeoutInterval = snapshot.isConnected ? 600.0 : 8.0

        for (name, value) in request.headers {
            let lower = name.lowercased()
            if !Self.strippedRequestHeaders.contains(lower) {
                urlReq.setValue(value, forHTTPHeaderField: name)
            }
        }

        if !snapshot.apiAccessKey.isEmpty {
            urlReq.setValue("Bearer \(snapshot.apiAccessKey)", forHTTPHeaderField: "Authorization")
            urlReq.setValue(snapshot.apiAccessKey, forHTTPHeaderField: "x-api-key")
        }

        if !finalBody.isEmpty || request.method == "POST" {
            urlReq.httpBody = finalBody
            if urlReq.value(forHTTPHeaderField: "Content-Type") == nil {
                urlReq.setValue("application/json", forHTTPHeaderField: "Content-Type")
            }
        }

        onRequestForwarded?()

        let task = upstreamSession.dataTask(with: urlReq)
        let taskID = task.taskIdentifier
        let relay = StreamTaskRelay(clientFD: clientFD, isAnthropic: isAnthropic, task: task)

        relayLock.lock()
        relays[taskID] = relay
        relayLock.unlock()

        relay.startDisconnectMonitor { [weak self, weak relay] in
            if let self {
                self.relayLock.lock()
                _ = self.relays.removeValue(forKey: taskID)
                self.relayLock.unlock()
            }
            relay?.cancelAndClose()
        }

        task.resume()
    }

    private static func normalizeModelFieldIfEmpty(_ body: Data) -> Data {
        guard !body.isEmpty,
              var json = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any] else {
            return body
        }
        let rawModel = (json["model"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let lower = rawModel.lowercased()
        if rawModel.isEmpty || lower == "auto" {
            json["model"] = Self.virtualModelID
            if let updated = try? JSONSerialization.data(withJSONObject: json) {
                return updated
            }
        }
        return body
    }

    private static func isAnthropicEndpoint(_ cleanPath: String) -> Bool {
        switch cleanPath {
        case "/messages", "/v1/messages", "/v1/v1/messages",
             "/messages/count_tokens", "/v1/messages/count_tokens", "/v1/v1/messages/count_tokens":
            return true
        default:
            return false
        }
    }

    // MARK: - URLSessionDataDelegate 流式回调

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
    ) {
        relayLock.lock()
        let relay = relays[dataTask.taskIdentifier]
        relayLock.unlock()

        guard let relay, let httpResp = response as? HTTPURLResponse else {
            completionHandler(.cancel)
            return
        }

        let status = httpResp.statusCode
        let contentType = (httpResp.value(forHTTPHeaderField: "Content-Type") ?? "application/json").trimmingCharacters(in: .whitespacesAndNewlines)
        let lowerCT = contentType.lowercased()

        // 当 Colab 实例已断开/释放时，Gradio FRP 隧道会返回 404/502/504 的 HTML 或纯文本提示页，转换为标准 503 JSON
        if (status == 404 || status == 502 || status == 503 || status == 504) && !lowerCT.contains("application/json") && !lowerCT.contains("text/event-stream") {
            relayLock.lock()
            relays.removeValue(forKey: dataTask.taskIdentifier)
            relayLock.unlock()

            let errData = Self.makeOfflineErrorJSON(
                isAnthropic: relay.isAnthropic,
                reason: L("云端实例未运行或已释放 (HTTP \(status))", "Cloud tunnel is offline or released (HTTP \(status))")
            )
            relay.sendOfflineErrorAndClose(body: errData)
            completionHandler(.cancel)
            return
        }

        if !relay.writeResponseHeaders(statusCode: status, contentType: contentType) {
            relayLock.lock()
            relays.removeValue(forKey: dataTask.taskIdentifier)
            relayLock.unlock()
            relay.cancelAndClose()
            completionHandler(.cancel)
            return
        }

        completionHandler(.allow)
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        guard !data.isEmpty else { return }
        relayLock.lock()
        let relay = relays[dataTask.taskIdentifier]
        relayLock.unlock()

        guard let relay else {
            dataTask.cancel()
            return
        }

        if !relay.writeBodyChunk(data) {
            relayLock.lock()
            relays.removeValue(forKey: dataTask.taskIdentifier)
            relayLock.unlock()
            // 本地客户端（如 Claude Code / Magpie）主动中断流式读取时，立即取消上游任务以释放 Colab GPU
            relay.cancelAndClose()
        }
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        relayLock.lock()
        let relay = relays.removeValue(forKey: task.taskIdentifier)
        relayLock.unlock()

        guard let relay else { return }

        if let error {
            if !relay.headersSent {
                let nsErr = error as NSError
                if nsErr.code != NSURLErrorCancelled {
                    let errData = Self.makeOfflineErrorJSON(
                        isAnthropic: relay.isAnthropic,
                        reason: error.localizedDescription
                    )
                    relay.sendOfflineErrorAndClose(body: errData)
                    return
                }
            }
            relay.closeSocketOnly()
        } else {
            relay.finishChunkedStreamAndClose()
        }
    }

    // MARK: - HTTP 响应输出工具

    fileprivate static func makeOfflineErrorJSON(isAnthropic: Bool, reason: String) -> Data {
        let message = L(
            "EasyColab 云端实例当前不可用（\(reason)）。请在 EasyColab 客户端点击「唤醒并部署」并将模型加载到显存。",
            "EasyColab cloud runtime is currently unavailable (\(reason)). Wake & deploy in EasyColab and load your model to VRAM."
        )
        let payload: [String: Any]
        if isAnthropic {
            payload = [
                "type": "error",
                "error": [
                    "type": "overloaded_error",
                    "message": message
                ]
            ]
        } else {
            payload = [
                "error": [
                    "message": message,
                    "type": "service_unavailable",
                    "code": "colab_runtime_offline"
                ]
            ]
        }
        return (try? JSONSerialization.data(withJSONObject: payload)) ?? Data("{\"error\":{\"message\":\"Offline\"}}".utf8)
    }

    fileprivate static func sendJSONResponse(fd: Int32, statusCode: Int, body: Data) {
        sendSimpleResponse(
            fd: fd,
            statusCode: statusCode,
            statusText: httpReasonPhrase(statusCode),
            contentType: "application/json; charset=utf-8",
            body: body
        )
    }

    fileprivate static func sendSimpleResponse(
        fd: Int32,
        statusCode: Int,
        statusText: String,
        contentType: String,
        body: Data
    ) {
        let header = [
            "HTTP/1.1 \(statusCode) \(statusText)",
            "Content-Type: \(contentType)",
            "Content-Length: \(body.count)",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, OPTIONS",
            "Access-Control-Allow-Headers: *",
            "Cache-Control: no-cache",
            "Connection: close",
            "",
            ""
        ].joined(separator: "\r\n")

        if writeAll(fd, Data(header.utf8)), !body.isEmpty {
            _ = writeAll(fd, body)
        }
    }

    fileprivate static func httpReasonPhrase(_ code: Int) -> String {
        switch code {
        case 200: return "OK"
        case 201: return "Created"
        case 204: return "No Content"
        case 400: return "Bad Request"
        case 401: return "Unauthorized"
        case 403: return "Forbidden"
        case 404: return "Not Found"
        case 422: return "Unprocessable Entity"
        case 429: return "Too Many Requests"
        case 500: return "Internal Server Error"
        case 502: return "Bad Gateway"
        case 503: return "Service Unavailable"
        case 504: return "Gateway Timeout"
        default: return "OK"
        }
    }

    fileprivate static func writeAll(_ fd: Int32, _ data: Data) -> Bool {
        if data.isEmpty { return true }
        return data.withUnsafeBytes { rawBuf -> Bool in
            guard let base = rawBuf.baseAddress else { return true }
            var offset = 0
            let total = rawBuf.count
            while offset < total {
                let written = Darwin.send(fd, base.advanced(by: offset), total - offset, 0)
                if written < 0 {
                    if errno == EINTR { continue }
                    return false
                }
                if written == 0 { return false }
                offset += written
            }
            return true
        }
    }
}

// MARK: - 单次上游流式转发上下文

private final class StreamTaskRelay {
    private let lock = NSLock()
    private var clientFD: Int32
    private var isClosed: Bool = false
    private var headersSentFlag: Bool = false
    private var readSource: DispatchSourceRead? = nil
    let isAnthropic: Bool
    private weak var task: URLSessionDataTask?

    var headersSent: Bool {
        lock.lock()
        defer { lock.unlock() }
        return headersSentFlag
    }

    init(clientFD: Int32, isAnthropic: Bool, task: URLSessionDataTask) {
        self.clientFD = clientFD
        self.isAnthropic = isAnthropic
        self.task = task
    }

    deinit {
        if !isClosed {
            let fd = clientFD
            let src = readSource
            isClosed = true
            clientFD = -1
            readSource = nil
            Self.releaseSocket(fd: fd, source: src)
        }
    }

    func startDisconnectMonitor(onDisconnect: @escaping () -> Void) {
        lock.lock()
        guard !isClosed, clientFD >= 0, readSource == nil else {
            lock.unlock()
            return
        }
        let fd = clientFD
        let source = DispatchSource.makeReadSource(
            fileDescriptor: fd,
            queue: DispatchQueue.global(qos: .utility)
        )
        var retainedSource: DispatchSourceRead? = source
        source.setEventHandler { [weak self] in
            self?.handleReadEvent(onDisconnect: onDisconnect)
        }
        source.setCancelHandler {
            // Single owner of Darwin.close(fd) once DispatchSourceRead is attached;
            // executed by libdispatch strictly after kqueue deregistration and any active eventHandler.
            Darwin.close(fd)
            _ = retainedSource
            retainedSource = nil
        }
        source.resume()
        self.readSource = source
        lock.unlock()
    }

    private func handleReadEvent(onDisconnect: () -> Void) {
        lock.lock()
        guard !isClosed, clientFD >= 0 else {
            lock.unlock()
            return
        }
        let fd = clientFD
        lock.unlock()

        var b: UInt8 = 0
        let peeked = Darwin.recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT)
        if peeked == 0 {
            // 客户端在长上下文预填充或流式解码中途主动关闭 TCP 连接 (EOF)，立即取消上游推理
            onDisconnect()
        } else if peeked < 0 {
            let err = errno
            if err != EAGAIN && err != EWOULDBLOCK && err != EINTR {
                onDisconnect()
            }
        } else {
            // 排空 HTTP 请求体尾部多余字节，避免水平触发的 kqueue 读事件空转
            var drain = [UInt8](repeating: 0, count: 512)
            while Darwin.recv(fd, &drain, drain.count, MSG_DONTWAIT) > 0 {}
        }
    }

    private static func releaseSocket(fd: Int32, source: DispatchSourceRead?) {
        if let source {
            source.cancel()
        } else if fd >= 0 {
            Darwin.close(fd)
        }
    }

    func writeResponseHeaders(statusCode: Int, contentType: String) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !isClosed, clientFD >= 0 else { return false }

        let reason = LocalAPIGatewayServer.httpReasonPhrase(statusCode)
        let headerBlock = [
            "HTTP/1.1 \(statusCode) \(reason)",
            "Content-Type: \(contentType)",
            "Transfer-Encoding: chunked",
            "Access-Control-Allow-Origin: *",
            "Access-Control-Allow-Methods: GET, POST, OPTIONS",
            "Access-Control-Allow-Headers: *",
            "Cache-Control: no-cache",
            "X-Accel-Buffering: no",
            "Connection: close",
            "",
            ""
        ].joined(separator: "\r\n")

        let ok = LocalAPIGatewayServer.writeAll(clientFD, Data(headerBlock.utf8))
        if ok {
            headersSentFlag = true
        }
        return ok
    }

    func writeBodyChunk(_ chunk: Data) -> Bool {
        guard !chunk.isEmpty else { return true }
        lock.lock()
        defer { lock.unlock() }
        guard !isClosed, clientFD >= 0, headersSentFlag else { return false }

        let sizePrefix = Data("\(String(chunk.count, radix: 16))\r\n".utf8)
        let crlf = Data("\r\n".utf8)
        guard LocalAPIGatewayServer.writeAll(clientFD, sizePrefix),
              LocalAPIGatewayServer.writeAll(clientFD, chunk),
              LocalAPIGatewayServer.writeAll(clientFD, crlf) else {
            return false
        }
        return true
    }

    func finishChunkedStreamAndClose() {
        lock.lock()
        let fd = clientFD
        let sent = headersSentFlag
        let src = readSource
        let alreadyClosed = isClosed
        isClosed = true
        clientFD = -1
        readSource = nil
        lock.unlock()

        guard !alreadyClosed, fd >= 0 else { return }
        if sent {
            _ = LocalAPIGatewayServer.writeAll(fd, Data("0\r\n\r\n".utf8))
        }
        Self.releaseSocket(fd: fd, source: src)
    }

    func sendOfflineErrorAndClose(body: Data) {
        lock.lock()
        let fd = clientFD
        let sent = headersSentFlag
        let src = readSource
        let alreadyClosed = isClosed
        isClosed = true
        clientFD = -1
        readSource = nil
        lock.unlock()

        guard !alreadyClosed, fd >= 0 else { return }
        if !sent {
            LocalAPIGatewayServer.sendJSONResponse(fd: fd, statusCode: 503, body: body)
        }
        Self.releaseSocket(fd: fd, source: src)
    }

    func cancelAndClose() {
        task?.cancel()
        closeSocketOnly()
    }

    func closeSocketOnly() {
        lock.lock()
        let fd = clientFD
        let src = readSource
        let alreadyClosed = isClosed
        isClosed = true
        clientFD = -1
        readSource = nil
        lock.unlock()

        guard !alreadyClosed, fd >= 0 else { return }
        Self.releaseSocket(fd: fd, source: src)
    }
}

// MARK: - HTTP/1.1 请求读取器（同时兼容 Content-Length 与 Go net/http 的 Transfer-Encoding: chunked）

private struct ParsedHTTPRequest {
    let method: String
    let target: String
    let headers: [(String, String)]
    let body: Data
}

private final class SocketByteReader {
    private let fd: Int32
    private var buffer = Data()
    private static let crlf = Data("\r\n".utf8)
    private static let headerTerminator = Data("\r\n\r\n".utf8)
    private static let maxHeaderBytes = 65536
    private static let maxBodyBytes = 64 * 1024 * 1024 // 最高支持 64MB (含多模态高清 base64 图片输入)

    init(fd: Int32) {
        self.fd = fd
    }

    private func fillOnce() -> Bool {
        var temp = [UInt8](repeating: 0, count: 16384)
        while true {
            let n = Darwin.recv(fd, &temp, temp.count, 0)
            if n > 0 {
                buffer.append(temp, count: n)
                return true
            }
            if n < 0 && errno == EINTR {
                continue
            }
            return false
        }
    }

    private func readHeaderData() -> Data? {
        while true {
            if let range = buffer.range(of: Self.headerTerminator) {
                let headerSlice = Data(buffer[buffer.startIndex..<range.lowerBound])
                buffer.removeSubrange(buffer.startIndex..<range.upperBound)
                return headerSlice
            }
            if buffer.count > Self.maxHeaderBytes {
                return nil
            }
            if !fillOnce() {
                return nil
            }
        }
    }

    private func readExactBytes(_ count: Int) -> Data? {
        guard count >= 0, count <= Self.maxBodyBytes else { return nil }
        if count == 0 { return Data() }
        while buffer.count < count {
            if !fillOnce() {
                return nil
            }
        }
        let out = Data(buffer.prefix(count))
        buffer.removeFirst(count)
        return out
    }

    private func readLine() -> String? {
        while true {
            if let range = buffer.range(of: Self.crlf) {
                let lineData = Data(buffer[buffer.startIndex..<range.lowerBound])
                buffer.removeSubrange(buffer.startIndex..<range.upperBound)
                return String(data: lineData, encoding: .utf8) ?? String(decoding: lineData, as: UTF8.self)
            }
            if buffer.count > 8192 {
                return nil
            }
            if !fillOnce() {
                return nil
            }
        }
    }

    private func readChunkedBody() -> Data? {
        var result = Data()
        while true {
            guard let sizeLine = readLine() else { return nil }
            let hexPart = sizeLine.split(separator: ";", maxSplits: 1).first.map(String.init)?
                .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard let chunkSize = Int(hexPart, radix: 16), chunkSize >= 0 else {
                return nil
            }
            if chunkSize == 0 {
                // 消费尾部空行或可选 Trailer 头
                while let trailer = readLine() {
                    if trailer.isEmpty { break }
                }
                return result
            }
            if result.count + chunkSize > Self.maxBodyBytes {
                return nil
            }
            guard let chunkData = readExactBytes(chunkSize),
                  let _ = readExactBytes(2) else {
                return nil
            }
            result.append(chunkData)
        }
    }

    func readHTTPRequest() -> ParsedHTTPRequest? {
        guard let headerData = readHeaderData(),
              let headerText = String(data: headerData, encoding: .utf8) else {
            return nil
        }

        let lines = headerText.components(separatedBy: "\r\n")
        guard let requestLine = lines.first else { return nil }
        let parts = requestLine.split(separator: " ", maxSplits: 2).map(String.init)
        guard parts.count >= 2 else { return nil }

        let method = parts[0].uppercased()
        let target = parts[1]

        var headers: [(String, String)] = []
        var contentLength = 0
        var isChunked = false
        var expectsContinue = false

        for line in lines.dropFirst() {
            guard let colonIdx = line.firstIndex(of: ":") else { continue }
            let name = String(line[..<colonIdx]).trimmingCharacters(in: .whitespaces)
            let value = String(line[line.index(after: colonIdx)...]).trimmingCharacters(in: .whitespaces)
            headers.append((name, value))

            let lowName = name.lowercased()
            if lowName == "content-length", let n = Int(value), n > 0 {
                contentLength = n
            } else if lowName == "transfer-encoding", value.lowercased().contains("chunked") {
                isChunked = true
            } else if lowName == "expect", value.lowercased().contains("100-continue") {
                expectsContinue = true
            }
        }

        if expectsContinue {
            _ = LocalAPIGatewayServer.writeAll(fd, Data("HTTP/1.1 100 Continue\r\n\r\n".utf8))
        }

        let body: Data
        if isChunked {
            guard let decoded = readChunkedBody() else { return nil }
            body = decoded
        } else if contentLength > 0 {
            guard let exact = readExactBytes(contentLength) else { return nil }
            body = exact
        } else {
            body = Data()
        }

        return ParsedHTTPRequest(method: method, target: target, headers: headers, body: body)
    }
}
