/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hadoop.io.compress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.brotli.BrotliCompressor;
import org.apache.hadoop.io.compress.brotli.BrotliDecompressor;
import org.meteogroup.jbrotli.libloader.BrotliLibraryLoader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BrotliCodec extends Configured implements CompressionCodec {
  public static final String IS_TEXT_PROP = "compression.brotli.is-text";
  public static final String QUALITY_LEVEL_PROP = "compression.brotli.quality";

  private static boolean LOADED_NATIVE_LIB = false;

  public BrotliCodec() {
    if (!BrotliCodec.LOADED_NATIVE_LIB) {
      synchronized (BrotliCodec.class) {
        if (!BrotliCodec.LOADED_NATIVE_LIB) {
          BrotliLibraryLoader.loadBrotli();
          BrotliCodec.LOADED_NATIVE_LIB = true;
        }
      }
    }
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    return new BrotliCompressorStream(out, compressor);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return BrotliCompressor.class;
  }

  @Override
  public Compressor createCompressor() {
    return new BrotliCompressor(getConf());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
      throws IOException {
    return new BrotliDecompressorStream(in, decompressor);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return BrotliDecompressor.class;
  }

  @Override
  public Decompressor createDecompressor() {
    return new BrotliDecompressor();
  }

  @Override
  public String getDefaultExtension() {
    return ".br";
  }

  private static class BrotliCompressorStream extends CompressorStream {
    public BrotliCompressorStream(OutputStream out, Compressor compressor, int bufferSize) {
      super(out, compressor, bufferSize);
    }

    public BrotliCompressorStream(OutputStream out, Compressor compressor) {
      super(out, compressor);
    }

    @Override
    public void close() throws IOException {
      super.close();
      compressor.end();
    }
  }

  private static class BrotliDecompressorStream extends DecompressorStream {
    public BrotliDecompressorStream(InputStream in, Decompressor decompressor, int bufferSize) throws IOException {
      super(in, decompressor, bufferSize);
    }

    public BrotliDecompressorStream(InputStream in, Decompressor decompressor) throws IOException {
      super(in, decompressor);
    }

    @Override
    public void close() throws IOException {
      super.close();
      decompressor.end();
    }
  }
}
