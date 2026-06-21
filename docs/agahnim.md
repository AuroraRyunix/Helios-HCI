# Agahnim: Native Console Proxy Sidecar & WebGL Renderer

**Agahnim** is a high-performance, native Rust sidecar service and rendering specification designed to replace the Python-based WebSocket console proxy and hardware-accelerate front-end canvas painting.

In Zelda lore, *Agahnim* is Ganon's shadow proxy in the Light World. Similarly, this service acts as the lightweight, ultra-fast proxy bridging browser client connections to the underlying isolated VM console TCP sockets.

---

## 1. Option 3: Rust Proxy Sidecar Architecture

### Background
Currently, WebSocket connections for VM consoles (VNC/SPICE) are handled by a synchronous, single-threaded Python `select.select()` loop in `spectrum_server.py`. This holds a thread open for the entire duration of the console session and incurs significant CPU and latency overhead under heavy packet traffic.

### Architecture (Direct Client-to-Daemon Bypass)
To achieve maximum performance and zero-copy latency, the browser client **bypasses the Python backend (`Spectrum`) completely** for the transit of console frame data.

Instead of reverse-proxying WebSocket traffic through the Python HTTP daemon, the WebUI client performs a direct connection to the native Rust daemon `Agahnim` running on port `8081` on the target hypervisor node.

```
[ Browser Client ] ──────────────────(WebSocket)──────────────────┐
       │                                                          │
       │ 1. Get connection coordinates                            │ 2. Direct Console Stream
       ▼                                                          ▼
[ Spectrum Gateway (Python) ]                                [ Agahnim Daemon (Rust) ]
                                                                  │
                                                                  │ (TCP Bridge on localhost)
                                                                  ▼
                                                       [ QEMU Guest VNC/SPICE ]
```

### Specifications
* **Language & Runtime**: Rust (stable), leveraging `tokio` for non-blocking asynchronous event loops and `tokio-tungstenite` for WebSocket protocol upgrades and frame parsing.
* **Dynamic Resolution**:
  * Upon receiving a connection on `/ws?name=VM_NAME&type=spice_or_vnc`, the daemon executes the local CLI command `virsh domdisplay <name>` (for SPICE) or `virsh vncdisplay <name>` (for VNC) to determine the VM's active TCP port.
  * Alternatively, it queries the local ScyllaDB cluster settings to locate target hypervisor assignments.
* **High-Concurrency Relaying**:
  * Spawns lightweight green threads (async tasks) to handle bidirectional byte copying (`tokio::io::copy_bidirectional`) between the WebSocket stream and the TCP target socket.
  * Replaces locking Python select loops with low-overhead kernel polling, reducing CPU usage to near 0% when idle and minimizing transit latency.

---

## 2. Option 4: WebGL Canvas Rendering

### Background
The standard SPICE client (`display.js`) uses a 2D HTML5 Canvas context. During heavy screen updates (scrolling, video playback) on Retina or High-DPI screens, the CPU-bound `drawImage` and pixel scaling operations lock the browser's main thread.

### Specifications
* **GPU-Accelerated Textures**:
  * Instead of painting decoded frames onto a Canvas 2D context using `putImageData` or `drawImage`, the front-end will compile a WebGL program.
  * Decompressed frame buffers from `lz_worker.js` are uploaded directly to the GPU as a 2D texture (`gl.texImage2D`).
* **Hardware-Accelerated Scaling**:
  * Hardware bilinear filtering scales the 1080p frame up to Retina/4K resolutions automatically on the GPU.
  * Shaders map the texture to a simple quad spanning the canvas viewport.
* **Vertex and Fragment Shaders**:
  ```glsl
  // Vertex Shader
  attribute vec2 position;
  varying vec2 texCoord;
  void main() {
      texCoord = vec2(position.x * 0.5 + 0.5, 0.5 - position.y * 0.5);
      gl_Position = vec4(position, 0.0, 1.0);
  }
  ```
  ```glsl
  // Fragment Shader
  precision mediump float;
  varying vec2 texCoord;
  uniform sampler2D u_texture;
  void main() {
      gl_FragColor = texture2D(u_texture, texCoord);
  }
  ```

---

## 3. Deployment & Integration Plan

### Daemon Setup (Backend)
1. Install Rust compiler toolchain on the target nodes via DNF:
   ```bash
   dnf install -y rust cargo
   ```
2. Build the `agahnim` binary:
   ```bash
   cargo build --release
   ```
3. Configure a systemd service unit `agahnim.service`:
   ```ini
   [Unit]
   Description=Agahnim Console Proxy Daemon
   After=network.target

   [Service]
   Type=simple
   ExecStart=/usr/local/bin/agahnim --port 8081
   Restart=always
   RestartSec=3
   User=root
   CPUWeight=100
   MemoryMax=256M

   [Install]
   WantedBy=multi-user.target
   ```

### Integration & Coordinate API (Spectrum Gateway)
Rather than proxying, the `/api/vms/console/ws` route in `spectrum_server.py` is changed to a fast HTTP coordinate lookup endpoint. 
1. The client requests the coordinates for a VM's console.
2. The server queries the database/hypervisor, resolves the active VM host IP and the target port, and returns the direct WebSocket connection string:
   ```json
   {
     "url": "ws://<host_ip>:8081/ws?name=VM_NAME&type=spice"
   }
   ```
3. The browser client immediately opens a WebSocket connection directly to the resolved `Agahnim` daemon. This keeps Python completely out of the hot path.
