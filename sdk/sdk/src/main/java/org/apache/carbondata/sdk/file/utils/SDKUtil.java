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

package org.apache.carbondata.sdk.file.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.orc.FileFormatException;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

public class SDKUtil {
  public static ArrayList listFiles(String sourceFolder, final String suf) {
    return listFiles(sourceFolder, suf, new Configuration(true));
  }

  public static ArrayList listFiles(String sourceImageFolder,
                                    final String suf, Configuration conf) {
    final String sufImageFinal = suf;
    ArrayList result = new ArrayList();
    CarbonFile[] fileList = FileFactory.getCarbonFile(sourceImageFolder, conf).listFiles();
    for (int i = 0; i < fileList.length; i++) {
      if (fileList[i].isDirectory()) {
        result.addAll(listFiles(fileList[i].getCanonicalPath(), sufImageFinal, conf));
      } else if (fileList[i].getCanonicalPath().endsWith(sufImageFinal)) {
        result.add(fileList[i].getCanonicalPath());
      }
    }
    return result;
  }

  public static Object[] getSplitList(String path, String suf,
                                      int numOfSplit, Configuration conf) {
    List fileList = listFiles(path, suf, conf);
    List splitList = new ArrayList<List>();
    if (numOfSplit < fileList.size()) {
      // If maxSplits is less than the no. of files
      // Split the reader into maxSplits splits with each
      // element containing >= 1 CarbonRecordReader objects
      float filesPerSplit = (float) fileList.size() / numOfSplit;
      for (int i = 0; i < numOfSplit; ++i) {
        splitList.add(fileList.subList(
            (int) Math.ceil(i * filesPerSplit),
            (int) Math.ceil(((i + 1) * filesPerSplit))));
      }
    } else {
      // If maxSplits is greater than the no. of files
      // Split the reader into <num_files> splits with each
      // element contains exactly 1 CarbonRecordReader object
      for (int i = 0; i < fileList.size(); ++i) {
        splitList.add((fileList.subList(i, i + 1)));
      }
    }
    return splitList.toArray();
  }

  public static Object[] getSplitList(String path, String suf,
                                      int numOfSplit) {
    return getSplitList(path, suf, numOfSplit, new Configuration());
  }

  public static Object[] readObjects(Object[] input, int i) {
    return (Object[]) input[i];
  }

  public static List<CarbonFile> extractFilesFromFolder(String path,
      String suf, Configuration hadoopConf) {
    List dataFiles = listFiles(path, suf, hadoopConf);
    List<CarbonFile> carbonFiles = new ArrayList<>();
    for (Object dataFile: dataFiles) {
      carbonFiles.add(FileFactory.getCarbonFile(dataFile.toString(), hadoopConf));
    }
    if (CollectionUtils.isEmpty(dataFiles)) {
      throw new RuntimeException("No file found at given location. Please provide" +
          "the correct folder location.");
    }
    return carbonFiles;
  }

  public static DataFileStream<GenericData.Record> buildAvroReader(CarbonFile carbonFile,
       Configuration configuration) throws IOException {
    try {
      GenericDatumReader<GenericData.Record> genericDatumReader =
          new GenericDatumReader<>();
      DataFileStream<GenericData.Record> avroReader =
          new DataFileStream<>(FileFactory.getDataInputStream(carbonFile.getPath(),
          -1, configuration), genericDatumReader);
      return avroReader;
    } catch (FileNotFoundException ex) {
      throw new FileNotFoundException("File " + carbonFile.getPath()
          + " not found to build carbon writer.");
    } catch (IOException ex) {
      if (ex.getMessage().contains("Not a data file")) {
        throw new RuntimeException("File " + carbonFile.getPath() + " is not in avro format.");
      } else {
        throw ex;
      }
    }
  }

  public static Reader buildOrcReader(String path, Configuration conf) throws IOException {
    try {
      Reader orcReader = OrcFile.createReader(new Path(path),
          OrcFile.readerOptions(conf));
      return orcReader;
    } catch (FileFormatException ex) {
      throw new RuntimeException("File " + path + " is not in ORC format");
    } catch (FileNotFoundException ex) {
      throw new FileNotFoundException("File " + path + " not found to build carbon writer.");
    }
  }

  public static ParquetReader<GenericRecord> buildParquetReader(String path, Configuration conf)
      throws IOException {
    try {
      AvroReadSupport<GenericRecord> avroReadSupport = new AvroReadSupport<>();
      ParquetReader<GenericRecord> parquetReader = ParquetReader.builder(avroReadSupport,
          new Path(path)).withConf(conf).build();
      return parquetReader;
    } catch (FileNotFoundException ex) {
      throw new FileNotFoundException("File " + path + " not found to build carbon writer.");
    }
  }

  public static CsvParser buildCsvParser(Configuration conf) {
    CsvParserSettings settings = CSVInputFormat.extractCsvParserSettings(conf);
    return new CsvParser(settings);
  }

  public static java.io.Reader buildJsonReader(CarbonFile file, Configuration conf)
      throws IOException {
    InputStream inputStream = FileFactory.getDataInputStream(file.getPath(), -1, conf);
    java.io.Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    return reader;
  }

}
