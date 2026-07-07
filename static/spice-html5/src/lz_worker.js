"use strict";

var wasmInstance = null;
var wasmMemory = null;
var wasmPromise = null;

function loadWasm(wasmUrl) {
    if (wasmPromise) return wasmPromise;
    try {
        wasmPromise = fetch(wasmUrl)
            .then(function(response) {
                return response.arrayBuffer();
            })
            .then(function(bytes) {
                wasmMemory = new WebAssembly.Memory({ initial: 256 }); // 16MB
                return WebAssembly.instantiate(bytes, {
                    env: {
                        memory: wasmMemory
                    }
                });
            })
            .then(function(results) {
                wasmInstance = results.instance;
                if (wasmInstance.exports.memory) {
                    wasmMemory = wasmInstance.exports.memory;
                }
                console.log("Web Worker: WASM SPICE LZ decompressor loaded successfully.");
            })
            .catch(function(err) {
                console.error("Web Worker: Failed to load WASM SPICE LZ decompressor, falling back to JS:", err);
            });
    } catch (e) {
        console.error("Web Worker: Error setting up WASM SPICE URL, falling back to JS:", e);
        wasmPromise = Promise.resolve();
    }
    return wasmPromise;
}

function lz_rgb32_decompress(in_buf, at, out_buf, type, default_alpha)
{
    var encoder = at;
    var op = 0;
    var ctrl;
    var i = 0;

    while ((op * 4) < out_buf.length)
    {
        ctrl = in_buf[encoder++];
        var ref = op;
        var len = ctrl >> 5;
        var ofs = (ctrl & 31) << 8;

        if (ctrl >= 32) {

            var code;
            len--;

            if (len == 7 - 1) {
                do {
                    code = in_buf[encoder++];
                    len += code;
                } while (code == 255);
            }
            code = in_buf[encoder++];
            ofs += code;


            if (code == 255) {
                if ((ofs - code) == (31 << 8)) {
                    ofs = in_buf[encoder++] << 8;
                    ofs += in_buf[encoder++];
                    ofs += 8191;
                }
            }
            len += 1;
            if (type == 9) // Constants.LZ_IMAGE_TYPE_RGBA
                len += 2;

            ofs += 1;

            ref -= ofs;
            if (ref == (op - 1)) {
                var b = ref;
                for (; len; --len) {
                    if (type == 9)
                    {
                        out_buf[(op*4) + 3] = out_buf[(b*4)+3];
                    }
                    else
                    {
                        for (i = 0; i < 4; i++)
                            out_buf[(op*4) + i] = out_buf[(b*4)+i];
                    }
                    op++;
                }
            } else {
                for (; len; --len) {
                    if (type == 9)
                    {
                        out_buf[(op*4) + 3] = out_buf[(ref*4)+3];
                    }
                    else
                    {
                        for (i = 0; i < 4; i++)
                            out_buf[(op*4) + i] = out_buf[(ref*4)+i];
                    }
                    op++; ref++;
                }
            }
        } else {
            ctrl++;

            if (type == 9)
            {
                out_buf[(op*4) + 3] = in_buf[encoder++];
            }
            else
            {
                out_buf[(op*4) + 0] = in_buf[encoder + 2];
                out_buf[(op*4) + 1] = in_buf[encoder + 1];
                out_buf[(op*4) + 2] = in_buf[encoder + 0];
                if (default_alpha)
                    out_buf[(op*4) + 3] = 255;
                encoder += 3;
            }
            op++;


            for (--ctrl; ctrl; ctrl--) {
                if (type == 9)
                {
                    out_buf[(op*4) + 3] = in_buf[encoder++];
                }
                else
                {
                    out_buf[(op*4) + 0] = in_buf[encoder + 2];
                    out_buf[(op*4) + 1] = in_buf[encoder + 1];
                    out_buf[(op*4) + 2] = in_buf[encoder + 0];
                    if (default_alpha)
                        out_buf[(op*4) + 3] = 255;
                    encoder += 3;
                }
                op++;
            }
        }

    }
    return encoder;
}

function flip_image_data(data, width, height)
{
    var wb = width * 4;
    var h = height;
    var temp_h = h;
    var buff = new Uint8Array(width * height * 4);
    while (temp_h--)
    {
        buff.set(data.subarray(temp_h * wb, (temp_h + 1) * wb), (h - temp_h - 1) * wb);
    }
    data.set(buff);
}

function handleDecompress(req) {
    var lz_image = req.lz_image;
    var id = req.id;
    var u8 = new Uint8Array(lz_image.data);
    var width = lz_image.width;
    var height = lz_image.height;
    var type = lz_image.type;
    var top_down = lz_image.top_down;

    var decompressedLength = width * height * 4;
    var out_buf = new Uint8ClampedArray(decompressedLength);
    var at;

    if (type === 10) { // Constants.LZ_IMAGE_TYPE_XXXA
        if (wasmInstance && wasmMemory) {
            var in_ptr = 0;
            var out_ptr = 4 * 1024 * 1024; // 4MB
            var required_mem = out_ptr + decompressedLength;
            if (wasmMemory.buffer.byteLength < required_mem) {
                var pages_needed = Math.ceil((required_mem - wasmMemory.buffer.byteLength) / 65536);
                wasmMemory.grow(pages_needed);
            }
            
            var wasm_in_buf = new Uint8Array(wasmMemory.buffer, in_ptr, u8.length);
            wasm_in_buf.set(u8);
            
            wasmInstance.exports.lz_rgb32_decompress(
                in_ptr,
                0,
                out_ptr,
                decompressedLength,
                1, // RGBA = 1
                0  // default_alpha = 0 (false)
            );
            
            var wasm_out_buf = new Uint8ClampedArray(wasmMemory.buffer, out_ptr, decompressedLength);
            out_buf.set(wasm_out_buf);
        } else {
            lz_rgb32_decompress(u8, 0, out_buf, 9, false); // RGBA = 9
        }
    } else { // LZ_IMAGE_TYPE_RGB32 (8) or LZ_IMAGE_TYPE_RGBA (9)
        if (wasmInstance && wasmMemory) {
            var in_ptr = 0;
            var out_ptr = 4 * 1024 * 1024; // 4MB
            var required_mem = out_ptr + decompressedLength;
            if (wasmMemory.buffer.byteLength < required_mem) {
                var pages_needed = Math.ceil((required_mem - wasmMemory.buffer.byteLength) / 65536);
                wasmMemory.grow(pages_needed);
            }
            
            var wasm_in_buf = new Uint8Array(wasmMemory.buffer, in_ptr, u8.length);
            wasm_in_buf.set(u8);
            
            if (type === 9) {
                var next_at = wasmInstance.exports.lz_rgb32_decompress(
                    in_ptr,
                    0,
                    out_ptr,
                    decompressedLength,
                    0,
                    0
                );
                wasmInstance.exports.lz_rgb32_decompress(
                    in_ptr,
                    next_at,
                    out_ptr,
                    decompressedLength,
                    1,
                    0
                );
            } else {
                wasmInstance.exports.lz_rgb32_decompress(
                    in_ptr,
                    0,
                    out_ptr,
                    decompressedLength,
                    0,
                    1
                );
            }
            
            var wasm_out_buf = new Uint8ClampedArray(wasmMemory.buffer, out_ptr, decompressedLength);
            out_buf.set(wasm_out_buf);
        } else {
            at = lz_rgb32_decompress(u8, 0, out_buf, 8, type != 9); // RGB32 = 8
            if (type == 9) // RGBA = 9
                lz_rgb32_decompress(u8, at, out_buf, 9, false);
        }
    }

    if (!top_down) {
        flip_image_data(out_buf, width, height);
    }

    self.postMessage({
        type: 'decompressed',
        id: id,
        width: width,
        height: height,
        data: out_buf.buffer
    }, [out_buf.buffer]);
}

self.onmessage = function(e) {
    var data = e.data;
    if (data.type === 'init') {
        loadWasm(data.wasmUrl);
    } else if (data.type === 'decompress') {
        if (wasmPromise) {
            wasmPromise.then(function() {
                handleDecompress(data);
            });
        } else {
            handleDecompress(data);
        }
    }
};
