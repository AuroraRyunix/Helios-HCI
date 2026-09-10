#!/usr/bin/env python3
"""The vendored SPICE client's cursor encoder has to emit a stream a real inflater reads.

`create_rgba_png` in `static/spice-html5/src/png.js` builds the PNG for a remote cursor
by hand -- one stored deflate block wrapped in a zlib header and an Adler-32 -- and hands
it to the browser as a `data:image/png` URL. Nothing reports a malformed stream: the
browser drops the rule, the cursor keeps whatever shape it had, and the console looks like
it is working.

The encoder had both halves of the stored-block framing wrong at once. BFINAL went into
bit 7 of the deflate header instead of bit 0, and LEN/NLEN were written big endian --
which is what a DataView does unless you ask otherwise, and the opposite of what deflate
requires. The two are independent and either alone is fatal, so a test that read the
emitted bytes back and compared them against expected values would have gone green on a
file that still produced nothing an inflater would accept.

So these tests do not look at the bytes. They run the real png.js under node, feed what it
produced to Python's zlib, and compare the pixels that come out with the pixels that went
in. Only a stream correct in every respect survives that: zlib checks the block framing,
the declared lengths and the Adler-32, and comparing pixels catches a stream that inflates
to the wrong thing.

Requires node on PATH -- the point is to exercise the shipped JavaScript rather than a
Python restatement of it, and a restatement would prove only that the restatement is
right.

Run with:  python -m unittest test_spice_cursor_png
"""

import binascii
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
PNG_JS = os.path.join(HERE, "static", "spice-html5", "src", "png.js")

NODE = shutil.which("node")

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

# Calls the encoder the way cursor.js calls it -- an ArrayBuffer of RGBA -- and writes
# back exactly what it returned. Kept here rather than in the tree so there is no second
# copy of the module path to drift.
DRIVER = """\
import { readFileSync } from 'node:fs';

const spec = JSON.parse(readFileSync(0, 'utf8'));
const { create_rgba_png } = await import(process.argv[2]);
const pixels = Uint8Array.from(spec.pixels);
process.stdout.write(create_rgba_png(spec.width, spec.height, pixels.buffer));
"""


def cursor_pixels(width, height):
    """An RGBA cursor bitmap whose every byte differs from its neighbours."""
    return [(i * 37 + 11) & 0xFF for i in range(width * height * 4)]


def decode_data_uri(text):
    """The bytes behind what create_rgba_png returns, which is percent-escaped."""
    out = bytearray()
    i = 0
    while i < len(text):
        if text[i] == "%":
            out.append(int(text[i + 1:i + 3], 16))
            i += 3
        else:
            out.append(ord(text[i]))
            i += 1
    return bytes(out)


def png_chunks(data):
    """(type, payload, declared_crc, computed_crc) for each chunk after the signature."""
    if not data.startswith(PNG_SIGNATURE):
        raise ValueError("not a PNG: signature is %r" % data[:8])
    at = len(PNG_SIGNATURE)
    chunks = []
    while at < len(data):
        length = int.from_bytes(data[at:at + 4], "big")
        kind = data[at + 4:at + 8]
        payload = data[at + 8:at + 8 + length]
        declared = int.from_bytes(data[at + 8 + length:at + 12 + length], "big")
        chunks.append((kind, payload, declared, binascii.crc32(kind + payload)))
        at += 12 + length
    return chunks


def idat(data):
    joined = b"".join(p for kind, p, _, _ in png_chunks(data) if kind == b"IDAT")
    if not joined:
        raise ValueError("no IDAT chunk")
    return joined


def unfilter(raw, width, height):
    """Undo the per-row PNG filter. The encoder only ever emits filter type 0, None."""
    stride = width * 4
    rows = []
    at = 0
    for _ in range(height):
        kind = raw[at]
        if kind != 0:
            raise ValueError("unexpected filter type %d" % kind)
        rows.append(raw[at + 1:at + 1 + stride])
        at += 1 + stride
    return rows


@unittest.skipUnless(NODE, "node is not on PATH")
class CursorPngRoundTrip(unittest.TestCase):
    """Inflate what the shipped encoder actually emitted and check the pixels come back."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.driver = os.path.join(cls.tmp.name, "drive_png.mjs")
        with open(cls.driver, "w", encoding="utf-8") as handle:
            handle.write(DRIVER)
        cls.png_url = pathlib.Path(PNG_JS).as_uri()

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def encode(self, width, height, pixels):
        """The bytes the real create_rgba_png produced for this cursor."""
        spec = json.dumps({"width": width, "height": height, "pixels": pixels})
        result = subprocess.run(
            [NODE, self.driver, self.png_url],
            input=spec, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(0, result.returncode,
                         "the encoder did not run: %s" % result.stderr.strip())
        return decode_data_uri(result.stdout)

    def inflate(self, data):
        """The IDAT of `data`, decompressed, or a failure naming what zlib refused."""
        try:
            return zlib.decompress(idat(data))
        except zlib.error as err:
            self.fail("the emitted zlib stream does not decompress: %s" % err)

    def assert_round_trips(self, width, height):
        pixels = cursor_pixels(width, height)
        rows = unfilter(self.inflate(self.encode(width, height, pixels)), width, height)
        self.assertEqual(height, len(rows))
        stride = width * 4
        for y, row in enumerate(rows):
            self.assertEqual(bytes(pixels[y * stride:(y + 1) * stride]), row,
                             "row %d came back changed" % y)

    def test_cursor_bitmap_survives_the_round_trip(self):
        """A cursor-sized bitmap inflates back to the bytes that were handed in."""
        self.assert_round_trips(16, 16)

    def test_non_square_cursor_survives_the_round_trip(self):
        """Rows and columns are not interchangeable, so a square proves less than this."""
        self.assert_round_trips(11, 5)

    def test_single_pixel_cursor_survives_the_round_trip(self):
        """The smallest stored block there is -- five bytes of payload."""
        self.assert_round_trips(1, 1)

    def test_the_deflate_stream_declares_itself_finished(self):
        """BFINAL has to be set: a stream still expecting blocks is not a whole image.

        This is the half that survives a fix to LEN/NLEN alone. With BFINAL clear the
        framing can be perfect and the inflater will still sit waiting for a block that
        never comes, having read the Adler-32 as the start of one.
        """
        data = self.encode(8, 8, cursor_pixels(8, 8))
        engine = zlib.decompressobj()
        engine.decompress(idat(data))
        self.assertTrue(engine.eof, "the deflate stream never reached a final block")
        self.assertEqual(b"", engine.unused_data,
                         "bytes were left over after the stream ended")

    def test_every_chunk_carries_the_crc_it_claims(self):
        """The container libpng reads first, which is where the reported errors surfaced."""
        data = self.encode(16, 16, cursor_pixels(16, 16))
        kinds = []
        for kind, _, declared, computed in png_chunks(data):
            kinds.append(kind)
            self.assertEqual(declared, computed,
                             "%s chunk CRC is wrong" % kind.decode("ascii"))
        self.assertEqual([b"IHDR", b"IDAT", b"IEND"], kinds)


if __name__ == "__main__":
    unittest.main()
