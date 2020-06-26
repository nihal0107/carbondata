/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.sdk.file.utils.SDKUtil;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Implementation to write parquet rows in avro format to carbondata file.
 */
public class ParquetCarbonWriter extends AvroCarbonWriter {
  private Configuration configuration;
  private AvroCarbonWriter avroCarbonWriter = null;
  private CarbonFile[] dataFiles;

  ParquetCarbonWriter(AvroCarbonWriter avroCarbonWriter, Configuration configuration) {
    this.avroCarbonWriter = avroCarbonWriter;
    this.configuration = configuration;
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) {
    this.dataFiles = dataFiles;
  }

  /**
   * Load data of all parquet files at given location iteratively.
   *
   * @throws IOException
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withParquetPath()' " +
          "must be called to support loading parquet files");
    }
    if (this.avroCarbonWriter == null) {
      throw new RuntimeException("avro carbon writer can not be null");
    }
    Arrays.sort(this.dataFiles, Comparator.comparing(CarbonFile::getPath));
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    ParquetReader<GenericRecord> parquetReader =
        SDKUtil.buildParquetReader(file.getPath(), this.configuration);
    GenericRecord genericRecord;
    while ((genericRecord = parquetReader.read()) != null) {
      this.avroCarbonWriter.write(genericRecord);
    }
  }

  /**
   * Flush and close the writer
   */
  @Override
  public void close() throws IOException {
    try {
      this.avroCarbonWriter.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
