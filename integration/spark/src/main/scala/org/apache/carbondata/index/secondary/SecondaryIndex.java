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

package org.apache.carbondata.index.secondary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.cgindex.CoarseGrainIndex;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.index.secondary.SecondaryIndexModel.PositionReferenceInfo;

import org.apache.log4j.Logger;

import static org.apache.carbondata.core.util.path.CarbonTablePath.BATCH_PREFIX;

/**
 * Secondary Index to prune at blocklet level.
 */
public class SecondaryIndex extends CoarseGrainIndex {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SecondaryIndex.class.getName());
  private String indexName;
  private String currentSegmentId;
  private List<String> validSegmentIds;
  private PositionReferenceInfo positionReferenceInfo;

  @Override
  public void init(IndexModel indexModel) {
    assert (indexModel instanceof SecondaryIndexModel);
    SecondaryIndexModel model = (SecondaryIndexModel) indexModel;
    indexName = model.indexName;
    currentSegmentId = model.currentSegmentId;
    validSegmentIds = model.validSegmentIds;
    positionReferenceInfo = model.positionReferenceInfo;
  }

  private Set<String> getPositionReferences(String databaseName, String indexName,
      Expression expression) {
    /* If the position references are not obtained yet(i.e., prune happening for the first valid
    segment), then get them from the given index table with the given filter from all the valid
    segments at once and store them as map of segmentId to set of position references in that
    particular segment. Upon the subsequent prune for other segments, return the position
    references for the respective segment from the map directly */
    if (!positionReferenceInfo.isFetched()) {
      Object[] rows = IndexUtil.getPositionReferences(String
          .format("select distinct positionReference from %s.%s where insegment('%s') and %s",
              databaseName, indexName, String.join(",", validSegmentIds),
              expression.getStatement()));
      for (Object row : rows) {
        String positionReference = (String) row;
        int blockPathIndex = positionReference.indexOf("/");
        String segmentId = positionReference.substring(0, blockPathIndex);
        String blockPath = positionReference.substring(blockPathIndex + 1);
        Set<String> blockPaths = positionReferenceInfo.getSegmentToPosReferences()
            .computeIfAbsent(segmentId, k -> new HashSet<>());
        blockPaths.add(blockPath);
      }
      positionReferenceInfo.setFetched(true);
    }
    Set<String> blockPaths =
        positionReferenceInfo.getSegmentToPosReferences().get(currentSegmentId);
    return blockPaths != null ? blockPaths : new HashSet<>();
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      FilterExecutor filterExecutor, CarbonTable carbonTable) {
    Set<String> positionReferences = getPositionReferences(carbonTable.getDatabaseName(), indexName,
        filterExp.getFilterExpression());
    List<Blocklet> blocklets = new ArrayList<>();
    for (String blockPath : positionReferences) {
      int blockletIndex = blockPath.lastIndexOf("/");
      int taskNoStartIndex = blockPath.indexOf("-") + 1;
      int taskNoEndIndex = blockPath.indexOf("_");
      Blocklet blocklet = new Blocklet(
          blockPath.substring(taskNoStartIndex, taskNoEndIndex) + BATCH_PREFIX + blockPath
              .substring(taskNoEndIndex + 1, blockletIndex),
          blockPath.substring(blockletIndex + 1));
      blocklets.add(blocklet);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String
          .format("Secondary Index pruned blocklet count for segment %s is %d ", currentSegmentId,
              blocklets.size()));
    }
    return blocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
  }

  @Override
  public void finish() {
  }
}
