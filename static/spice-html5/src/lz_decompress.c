#define WASM_EXPORT __attribute__((visibility("default")))

WASM_EXPORT
int lz_rgb32_decompress(unsigned char* in_buf, int at, unsigned char* out_buf, int out_len, int type, int default_alpha) {
    int encoder = at;
    int op = 0;
    int ctrl;
    int i = 0;

    // Loop until we've decompressed the expected number of pixels.
    // out_len is the length of out_buf in bytes. Each pixel is 4 bytes.
    while ((op * 4) < out_len) {
        ctrl = in_buf[encoder++];
        int ref = op;
        int len = ctrl >> 5;
        int ofs = (ctrl & 31) << 8;

        if (ctrl >= 32) {
            unsigned char code;
            len--;

            if (len == 6) { // 7 - 1
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
            if (type == 1) { // LZ_IMAGE_TYPE_RGBA
                len += 2;
            }

            ofs += 1;
            ref -= ofs;

            if (ref == (op - 1)) {
                int b = ref;
                for (; len > 0; --len) {
                    if (type == 1) { // LZ_IMAGE_TYPE_RGBA
                        out_buf[(op * 4) + 3] = out_buf[(b * 4) + 3];
                    } else {
                        for (i = 0; i < 4; i++) {
                            out_buf[(op * 4) + i] = out_buf[(b * 4) + i];
                        }
                    }
                    op++;
                }
            } else {
                for (; len > 0; --len) {
                    if (type == 1) { // LZ_IMAGE_TYPE_RGBA
                        out_buf[(op * 4) + 3] = out_buf[(ref * 4) + 3];
                    } else {
                        for (i = 0; i < 4; i++) {
                            out_buf[(op * 4) + i] = out_buf[(ref * 4) + i];
                        }
                    }
                    op++;
                    ref++;
                }
            }
        } else {
            ctrl++;

            if (type == 1) { // LZ_IMAGE_TYPE_RGBA
                out_buf[(op * 4) + 3] = in_buf[encoder++];
            } else {
                out_buf[(op * 4) + 0] = in_buf[encoder + 2];
                out_buf[(op * 4) + 1] = in_buf[encoder + 1];
                out_buf[(op * 4) + 2] = in_buf[encoder + 0];
                if (default_alpha) {
                    out_buf[(op * 4) + 3] = 255;
                }
                encoder += 3;
            }
            op++;

            for (--ctrl; ctrl > 0; ctrl--) {
                if (type == 1) { // LZ_IMAGE_TYPE_RGBA
                    out_buf[(op * 4) + 3] = in_buf[encoder++];
                } else {
                    out_buf[(op * 4) + 0] = in_buf[encoder + 2];
                    out_buf[(op * 4) + 1] = in_buf[encoder + 1];
                    out_buf[(op * 4) + 2] = in_buf[encoder + 0];
                    if (default_alpha) {
                        out_buf[(op * 4) + 3] = 255;
                    }
                    encoder += 3;
                }
                op++;
            }
        }
    }
    return encoder;
}
