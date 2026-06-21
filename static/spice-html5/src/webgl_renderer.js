"use strict";

class WebGLContextEmulator {
    constructor(canvas) {
        this.canvas = canvas;
        const options = { preserveDrawingBuffer: true };
        const gl = canvas.getContext("webgl", options) || canvas.getContext("experimental-webgl", options);
        if (!gl) {
            throw new Error("WebGL not supported");
        }
        this.gl = gl;

        // Compile shaders
        const vsSource = `
            attribute vec2 a_position;
            uniform vec2 u_resolution;
            uniform vec4 u_rect;
            varying vec2 v_texCoord;
            void main() {
                vec2 zeroToOne = a_position * 0.5 + 0.5;
                vec2 pixelPos = u_rect.xy + zeroToOne * u_rect.zw;
                vec2 zeroToTwo = (pixelPos / u_resolution) * 2.0;
                vec2 ndcPos = zeroToTwo - 1.0;
                gl_Position = vec4(ndcPos.x, -ndcPos.y, 0.0, 1.0);
                v_texCoord = vec2(zeroToOne.x, zeroToOne.y);
            }
        `;

        const fsSource = `
            precision mediump float;
            varying vec2 v_texCoord;
            uniform sampler2D u_texture;
            uniform vec4 u_color;
            uniform bool u_use_texture;
            void main() {
                if (u_use_texture) {
                    gl_FragColor = texture2D(u_texture, v_texCoord);
                } else {
                    gl_FragColor = u_color;
                }
            }
        `;

        this.program = this.initShaderProgram(vsSource, fsSource);
        gl.useProgram(this.program);

        // Attributes and Uniforms
        this.positionAttributeLocation = gl.getAttribLocation(this.program, "a_position");
        this.resolutionUniformLocation = gl.getUniformLocation(this.program, "u_resolution");
        this.rectUniformLocation = gl.getUniformLocation(this.program, "u_rect");
        this.textureUniformLocation = gl.getUniformLocation(this.program, "u_texture");
        this.colorUniformLocation = gl.getUniformLocation(this.program, "u_color");
        this.useTextureUniformLocation = gl.getUniformLocation(this.program, "u_use_texture");

        // Setup vertex position buffer (unit quad)
        this.positionBuffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
        const positions = [
            -1.0, -1.0,
             1.0, -1.0,
            -1.0,  1.0,
            -1.0,  1.0,
             1.0, -1.0,
             1.0,  1.0,
        ];
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(positions), gl.STATIC_DRAW);

        // Setup texture
        this.texture = gl.createTexture();
        gl.bindTexture(gl.TEXTURE_2D, this.texture);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
        gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);

        // Enable alpha blending
        gl.enable(gl.BLEND);
        gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

        // Local state
        this._fillStyle = "rgb(0,0,0)";
        this.currentColorVec = [0, 0, 0, 1];
    }

    initShaderProgram(vsSource, fsSource) {
        const gl = this.gl;
        const vs = this.loadShader(gl.VERTEX_SHADER, vsSource);
        const fs = this.loadShader(gl.FRAGMENT_SHADER, fsSource);
        const program = gl.createProgram();
        gl.attachShader(program, vs);
        gl.attachShader(program, fs);
        gl.linkProgram(program);
        if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
            throw new Error("Unable to link shader program: " + gl.getProgramInfoLog(program));
        }
        return program;
    }

    loadShader(type, source) {
        const gl = this.gl;
        const shader = gl.createShader(type);
        gl.shaderSource(shader, source);
        gl.compileShader(shader);
        if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
            const info = gl.getShaderInfoLog(shader);
            gl.deleteShader(shader);
            throw new Error("An error occurred compiling shader: " + info);
        }
        return shader;
    }

    // fillStyle getter/setter
    get fillStyle() {
        return this._fillStyle;
    }

    set fillStyle(val) {
        this._fillStyle = val;
        this.currentColorVec = this.parseColor(val);
    }

    parseColor(colorStr) {
        const matches = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
        if (matches) {
            const r = parseInt(matches[1]) / 255.0;
            const g = parseInt(matches[2]) / 255.0;
            const b = parseInt(matches[3]) / 255.0;
            const a = matches[4] ? parseFloat(matches[4]) : 1.0;
            return [r, g, b, a];
        }
        return [0, 0, 0, 1];
    }

    save() {}
    restore() {}

    checkGLError(methodName) {
        const gl = this.gl;
        const err = gl.getError();
        if (err !== gl.NO_ERROR) {
            console.error(`WebGL Error in ${methodName}:`, err);
        }
    }

    fillRect(x, y, w, h) {
        const gl = this.gl;
        gl.viewport(0, 0, this.canvas.width, this.canvas.height);
        gl.useProgram(this.program);

        gl.uniform2f(this.resolutionUniformLocation, this.canvas.width, this.canvas.height);
        gl.uniform4f(this.rectUniformLocation, x, y, w, h);
        gl.uniform1i(this.useTextureUniformLocation, 0); // false (use solid color)
        gl.uniform4fv(this.colorUniformLocation, this.currentColorVec);

        gl.enableVertexAttribArray(this.positionAttributeLocation);
        gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
        gl.vertexAttribPointer(this.positionAttributeLocation, 2, gl.FLOAT, false, 0, 0);

        gl.drawArrays(gl.TRIANGLES, 0, 6);
        this.checkGLError("fillRect");
    }

    putImageData(imgData, x, y) {
        const gl = this.gl;
        gl.viewport(0, 0, this.canvas.width, this.canvas.height);
        gl.useProgram(this.program);

        // Bind texture and upload pixels
        gl.bindTexture(gl.TEXTURE_2D, this.texture);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, imgData);

        gl.uniform2f(this.resolutionUniformLocation, this.canvas.width, this.canvas.height);
        gl.uniform4f(this.rectUniformLocation, x, y, imgData.width, imgData.height);
        gl.uniform1i(this.useTextureUniformLocation, 1); // true (use texture)

        gl.enableVertexAttribArray(this.positionAttributeLocation);
        gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
        gl.vertexAttribPointer(this.positionAttributeLocation, 2, gl.FLOAT, false, 0, 0);

        gl.drawArrays(gl.TRIANGLES, 0, 6);
        this.checkGLError("putImageData");
    }

    drawImage(img, x, y, w, h) {
        const gl = this.gl;
        gl.viewport(0, 0, this.canvas.width, this.canvas.height);
        gl.useProgram(this.program);

        // Bind texture and upload pixels
        gl.bindTexture(gl.TEXTURE_2D, this.texture);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, img);

        const drawW = w !== undefined ? w : (img.width || img.naturalWidth || 0);
        const drawH = h !== undefined ? h : (img.height || img.naturalHeight || 0);

        gl.uniform2f(this.resolutionUniformLocation, this.canvas.width, this.canvas.height);
        gl.uniform4f(this.rectUniformLocation, x, y, drawW, drawH);
        gl.uniform1i(this.useTextureUniformLocation, 1); // true (use texture)

        gl.enableVertexAttribArray(this.positionAttributeLocation);
        gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer);
        gl.vertexAttribPointer(this.positionAttributeLocation, 2, gl.FLOAT, false, 0, 0);

        gl.drawArrays(gl.TRIANGLES, 0, 6);
        this.checkGLError("drawImage");
    }

    getImageData(x, y, w, h) {
        const gl = this.gl;
        const pixels = new Uint8Array(w * h * 4);
        
        // WebGL readPixels reads from bottom-left coordinate system.
        // Convert y coordinate to WebGL coordinates.
        const webglY = this.canvas.height - y - h;
        
        gl.readPixels(x, webglY, w, h, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
        
        // Flip the rows vertically
        const flippedPixels = new Uint8ClampedArray(w * h * 4);
        const rowBytes = w * 4;
        for (let row = 0; row < h; row++) {
            const srcRow = h - 1 - row;
            const srcOffset = srcRow * rowBytes;
            const destOffset = row * rowBytes;
            flippedPixels.set(pixels.subarray(srcOffset, srcOffset + rowBytes), destOffset);
        }
        
        return new ImageData(flippedPixels, w, h);
    }

    createImageData(w, h) {
        if (typeof w === 'object' && w !== null && 'width' in w && 'height' in w) {
            return new ImageData(w.width, w.height);
        }
        return new ImageData(w, h);
    }
}

export { WebGLContextEmulator };

