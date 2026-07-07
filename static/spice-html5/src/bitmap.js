"use strict";
/*
   Copyright (C) 2012 by Jeremy P. White <jwhite@codeweavers.com>

   This file is part of spice-html5.

   spice-html5 is free software: you can redistribute it and/or modify
   it under the terms of the GNU Lesser General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   spice-html5 is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public License
   along with spice-html5.  If not, see <http://www.gnu.org/licenses/>.
*/


/*----------------------------------------------------------------------------
**  bitmap.js
**      Handle SPICE_IMAGE_TYPE_BITMAP
**--------------------------------------------------------------------------*/

import { Constants } from './enums.js';

function convert_spice_bitmap_to_web(context, spice_bitmap)
{
    var ret;
    if (spice_bitmap.format != Constants.SPICE_BITMAP_FMT_32BIT &&
        spice_bitmap.format != Constants.SPICE_BITMAP_FMT_RGBA)
        return undefined;

    ret = context.createImageData(spice_bitmap.x, spice_bitmap.y);
    var u32_src = new Uint32Array(spice_bitmap.data);
    var u32_dst = new Uint32Array(ret.data.buffer);
    var width = spice_bitmap.x;
    var height = spice_bitmap.y;

    if (spice_bitmap.flags & Constants.SPICE_BITMAP_FLAGS_TOP_DOWN)
    {
        var len = width * height;
        if (spice_bitmap.format == Constants.SPICE_BITMAP_FMT_32BIT)
        {
            for (var i = 0; i < len; i++)
            {
                var pixel = u32_src[i];
                u32_dst[i] = 0xff000000 | ((pixel & 0xff) << 16) | (pixel & 0x0000ff00) | ((pixel >> 16) & 0xff);
            }
        }
        else
        {
            for (var i = 0; i < len; i++)
            {
                var pixel = u32_src[i];
                u32_dst[i] = (pixel & 0xff00ff00) | ((pixel & 0xff) << 16) | ((pixel >> 16) & 0xff);
            }
        }
    }
    else
    {
        var stride_pixels = spice_bitmap.stride / 4;
        for (var y = 0; y < height; y++)
        {
            var src_row_start = (height - 1 - y) * stride_pixels;
            var dst_row_start = y * width;
            if (spice_bitmap.format == Constants.SPICE_BITMAP_FMT_32BIT)
            {
                for (var x = 0; x < width; x++)
                {
                    var pixel = u32_src[src_row_start + x];
                    u32_dst[dst_row_start + x] = 0xff000000 | ((pixel & 0xff) << 16) | (pixel & 0x0000ff00) | ((pixel >> 16) & 0xff);
                }
            }
            else
            {
                for (var x = 0; x < width; x++)
                {
                    var pixel = u32_src[src_row_start + x];
                    u32_dst[dst_row_start + x] = (pixel & 0xff00ff00) | ((pixel & 0xff) << 16) | ((pixel >> 16) & 0xff);
                }
            }
        }
    }

    return ret;
}

export {
  convert_spice_bitmap_to_web,
};
