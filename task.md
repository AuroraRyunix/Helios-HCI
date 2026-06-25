# Tasks - Revert Web Worker & Fix Auto-Resize timing

- [x] Revert `display.js` to synchronous decompression (remove worker instantiation and message queue)
- [x] Consolidate the double-listeners for `toggleAutoscale` in `spice_auto.html`
- [x] Run full persistent deployment updates (`deploy_updates.py`)
- [x] Verify keyboard input and auto-resizing on VM `server2022`
- [x] Resolve VM live-migration reconciliation loop split-brain cleanup race condition
- [x] Implement VNC guest auto-resize (no scaling) with even pixel alignment, unconditional re-sync, and overflow hidden
- [x] Implement in-memory authentication session cache in `spectrum_server.py` to reduce console telemetry ping latency from 50ms to ~8ms
