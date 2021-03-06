/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.pubsubtobq.utils;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by the exercise pipelines.
 */
public interface Options extends PipelineOptions {

  @Description("Pub/Sub topic to read from. Used if --input is empty.")
  @Validation.Required
  String getTopic();

  void setTopic(String value);

  @Description("BigQuery Dataset to write tables to. Must already exist.")
  @Validation.Required
  String getOutputDataset();
  void setOutputDataset(String value);

  @Description("The BigQuery table name. Should not already exist.")
  @Validation.Required
  String getOutputTableName();

  void setOutputTableName(String value);
}
