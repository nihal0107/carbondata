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
import java.util.*;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.sdk.file.utils.SDKUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;


/**
 * Implementation to write ORC rows in CSV format to carbondata file.
 */
public class ORCCarbonWriter extends CSVCarbonWriter {
  private Configuration configuration;
  private CSVCarbonWriter csvCarbonWriter = null;
  private Reader orcReader = null;
  private CarbonFile[] dataFiles;

  ORCCarbonWriter(CSVCarbonWriter csvCarbonWriter, Configuration configuration) {
    this.csvCarbonWriter = csvCarbonWriter;
    this.configuration = configuration;
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) {
    this.dataFiles = dataFiles;
  }

  /**
   * Load ORC file in iterative way.
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withOrcPath()' must be called to support loading ORC files");
    }
    if (this.csvCarbonWriter == null) {
      throw new RuntimeException("csv carbon writer can not be null");
    }
    Arrays.sort(this.dataFiles, Comparator.comparing(CarbonFile::getPath));
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    orcReader = SDKUtil.buildOrcReader(file.getPath(), this.configuration);
    ObjectInspector objectInspector = orcReader.getObjectInspector();
    RecordReader recordReader = orcReader.rows();
    if (objectInspector instanceof StructObjectInspector) {
      StructObjectInspector structObjectInspector =
          (StructObjectInspector) orcReader.getObjectInspector();
      while (recordReader.hasNext()) {
        Object record = recordReader.next(null); // to remove duplicacy.
        List valueList = structObjectInspector.getStructFieldsDataAsList(record);
        for (int i = 0; i < valueList.size(); i++) {
          valueList.set(i, parseOrcObject(valueList.get(i), 0));
        }
        this.csvCarbonWriter.write(valueList.toArray());
      }
    } else {
      while (recordReader.hasNext()) {
        Object record = recordReader.next(null); // to remove duplicacy.
        this.csvCarbonWriter.write(new Object[]{parseOrcObject(record, 0)});
      }
    }
  }

  private String parseOrcObject(Object recordObject, int level) {
    if (recordObject instanceof OrcStruct) {
      Objects.requireNonNull(orcReader);
      StructObjectInspector structObjectInspector = (StructObjectInspector) orcReader
          .getObjectInspector();
      List value = structObjectInspector.getStructFieldsDataAsList(recordObject);
      for (int i = 0; i < value.size(); i++) {
        value.set(i, parseOrcObject(value.get(i), level + 1));
      }
      String str = listToString(value, level);
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    } else if (recordObject instanceof ArrayList) {
      ArrayList listValue = (ArrayList) recordObject;
      for (int i = 0; i < listValue.size(); i++) {
        listValue.set(i, parseOrcObject(listValue.get(i), level + 1));
      }
      String str = listToString(listValue, level);
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    } else if (recordObject instanceof LinkedHashMap) {
      LinkedHashMap<Text, Object> keyValueRow = (LinkedHashMap<Text, Object>) recordObject;
      for (Map.Entry<Text, Object> entry : keyValueRow.entrySet()) {
        Object val = parseOrcObject(keyValueRow.get(entry.getKey()), level + 2);
        keyValueRow.put(entry.getKey(), val);
      }
      StringBuilder str = new StringBuilder();
      for (Map.Entry<Text, Object> entry : keyValueRow.entrySet()) {
        Text key = entry.getKey();
        str.append(key.toString()).append("$").append(keyValueRow.get(key)).append("#");
      }
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    }
    if (recordObject == null) {
      return null;
    }
    return recordObject.toString();
  }

  private String listToString(List value, int level) {
    String delimiter = "";
    if (level == 0) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_1_DEFAULT;
    }
    else if (level == 1) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_2_DEFAULT;
    }
    else if (level == 2) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_3_DEFAULT;
    }
    else {
      throw new RuntimeException("carbon only support three level of ORC complex schema");
    }
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < value.size(); i++) {
      str.append(value.get(i)).append(delimiter);
    }
    return str.toString();
  }

  /**
   * Flush and close the writer
   */
  @Override
  public void close() throws IOException {
    try {
      this.csvCarbonWriter.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
