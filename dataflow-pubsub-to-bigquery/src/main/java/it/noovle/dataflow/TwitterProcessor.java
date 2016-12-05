/* Copyright 2016 Noovle Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package it.noovle.dataflow;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPI;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPIScopes;
import com.google.api.services.language.v1beta1.model.AnalyzeSentimentRequest;
import com.google.api.services.language.v1beta1.model.AnalyzeSentimentResponse;
import com.google.api.services.language.v1beta1.model.Document;
import com.google.api.services.language.v1beta1.model.Sentiment;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class TwitterProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterProcessor.class);

    // Constants
    private static final String APPLICATION_NAME = "TwitterProcessorDataflowPipeline/1.0";
    private static final String ANNOTATED_TWEETS_TABLE_SCHEMA =
            "{\"fields\":[{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"extended_entities\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"truncated\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_user_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_user_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_screen_name\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"location\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"description\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"protected\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"verified\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"followers_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"friends_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"listed_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favourites_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"statuses_count\",\"mode\":\"NULLABLE\"},{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"utc_offset\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"time_zone\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"geo_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"contributors_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"is_translator\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_background_tile\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_link_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_border_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_fill_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_text_color\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_use_background_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_banner_url\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"following\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"follow_request_sent\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"notifications\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"user\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"geo\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"coordinates\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"place_type\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"full_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country_code\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"bounding_box\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"attributes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"place\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"contributors\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"BOOLEAN\",\"name\":\"followers\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"scopes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"extended_entities\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"truncated\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_user_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_user_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_screen_name\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"location\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"description\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"protected\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"verified\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"followers_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"friends_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"listed_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favourites_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"statuses_count\",\"mode\":\"NULLABLE\"},{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"utc_offset\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"time_zone\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"geo_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"contributors_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"is_translator\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_background_tile\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_link_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_border_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_fill_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_text_color\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_use_background_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_banner_url\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"following\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"follow_request_sent\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"notifications\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"user\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"pl\",\"mode\":\"REPEATED\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"geo\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"coordinates\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"place_type\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"full_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country_code\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"bounding_box\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"attributes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"place\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"contributors\",\"mode\":\"REPEATED\"},{\"type\":\"INTEGER\",\"name\":\"retweet_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favorite_count\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"hashtags\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"trends\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"urls\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"user_mentions\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"symbols\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"entities\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"favorited\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"retweeted\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"timestamp_ms\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"possibly_sensitive\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"filter_level\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"withheld_copyright\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"retweeted_status\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"retweet_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favorite_count\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"hashtags\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"trends\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"urls\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"user_mentions\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"symbols\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"entities\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"favorited\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"retweeted\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"timestamp_ms\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"possibly_sensitive\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"filter_level\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"polarity\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"magnitude\",\"mode\":\"NULLABLE\"}]}";
    private static final String TWEETS_TABLE_SCHEMA =
            "{\"fields\":[{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"extended_entities\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"truncated\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_user_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_user_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_screen_name\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"location\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"description\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"protected\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"verified\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"followers_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"friends_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"listed_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favourites_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"statuses_count\",\"mode\":\"NULLABLE\"},{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"utc_offset\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"time_zone\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"geo_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"contributors_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"is_translator\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_background_tile\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_link_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_border_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_fill_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_text_color\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_use_background_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_banner_url\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"following\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"follow_request_sent\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"notifications\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"user\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"geo\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"coordinates\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"place_type\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"full_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country_code\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"bounding_box\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"attributes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"place\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"contributors\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"BOOLEAN\",\"name\":\"followers\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"scopes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"extended_entities\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"truncated\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_status_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"in_reply_to_user_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_user_id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"in_reply_to_screen_name\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"location\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"description\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"protected\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"verified\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"followers_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"friends_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"listed_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favourites_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"statuses_count\",\"mode\":\"NULLABLE\"},{\"type\":\"TIMESTAMP\",\"name\":\"created_at\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"utc_offset\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"time_zone\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"geo_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"contributors_enabled\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"is_translator\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_background_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_background_tile\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_link_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_border_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_sidebar_fill_color\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_text_color\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"profile_use_background_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_image_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"profile_banner_url\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"default_profile_image\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"following\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"follow_request_sent\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"notifications\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"user\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"pl\",\"mode\":\"REPEATED\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"geo\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"coordinates\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"place_type\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"full_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country_code\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"country\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"FLOAT\",\"name\":\"coordinates\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"bounding_box\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"attributes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"place\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"contributors\",\"mode\":\"REPEATED\"},{\"type\":\"INTEGER\",\"name\":\"retweet_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favorite_count\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"hashtags\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"trends\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"urls\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"user_mentions\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"symbols\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"entities\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"favorited\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"retweeted\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"timestamp_ms\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"possibly_sensitive\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"filter_level\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"withheld_copyright\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"retweeted_status\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"retweet_count\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"favorite_count\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"hashtags\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"trends\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"urls\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"STRING\",\"name\":\"screen_name\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"name\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"user_mentions\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"text\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"symbols\",\"mode\":\"REPEATED\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"id_str\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"indices\",\"mode\":\"REPEATED\"},{\"type\":\"STRING\",\"name\":\"media_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"media_url_https\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"display_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"expanded_url\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"type\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"large\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"medium\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"thumb\",\"mode\":\"NULLABLE\"},{\"fields\":[{\"type\":\"INTEGER\",\"name\":\"w\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"h\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"resize\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"small\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"sizes\",\"mode\":\"NULLABLE\"},{\"type\":\"INTEGER\",\"name\":\"source_status_id\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"source_status_id_str\",\"mode\":\"NULLABLE\"}],\"type\":\"RECORD\",\"name\":\"media\",\"mode\":\"REPEATED\"}],\"type\":\"RECORD\",\"name\":\"entities\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"favorited\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"retweeted\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"timestamp_ms\",\"mode\":\"NULLABLE\"},{\"type\":\"BOOLEAN\",\"name\":\"possibly_sensitive\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"filter_level\",\"mode\":\"NULLABLE\"},{\"type\":\"STRING\",\"name\":\"lang\",\"mode\":\"NULLABLE\"}]}";
    private static final Set<String> IGNORED_FIELDS = Sets.newHashSet("video_info", "scopes", "withheld_in_countries", "is_quote_status", "source_user_id", "quoted_status", "display_text_range", "quoted_status_id", "extended_tweet", "source_user_id_str", "quoted_status_id_str", "limit", "contributors", "withheld_copyright");

    /**
     * Converts a JSON String into a TableRow
     * <p>
     * This method converts a String containing a JSON into a TableRow object
     * that can be inserted into a BigQuery table
     *
     */
    private static final class DoFormat extends DoFn<String, TableRow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(DoFn<String, TableRow>.ProcessContext c) throws Exception {
            c.output(createTableRow(c.element()));
        }
    }

    /**
     * Filters tweets and processes them using the Natural Language API
     */
    private static final class DoFilterAndProcess extends DoFn<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
            try {
                JsonObject jsonTweet = new JsonParser().parse(c.element()).getAsJsonObject();

                if (jsonTweet != null && jsonTweet.getAsJsonPrimitive("text") != null && jsonTweet.getAsJsonPrimitive("lang") != null) {

                    // Process the element only if it contains "blackfriday" (even not as an hashtag)
                    if ((jsonTweet.getAsJsonPrimitive("text").getAsString().toLowerCase().contains("blackfriday")) && jsonTweet.getAsJsonPrimitive("lang").getAsString().equalsIgnoreCase("en")) {

                        LOG.info("Processing tweet: " + c.element());

                        CloudNaturalLanguageAPI languageService = getLanguageService();

                        Sentiment sentiment = analyzeSentiment(languageService, jsonTweet.getAsJsonPrimitive("text").getAsString());

                        jsonTweet.addProperty("polarity", sentiment.getPolarity());
                        jsonTweet.addProperty("magnitude", sentiment.getMagnitude());

                        c.output(jsonTweet.toString());

                    }
                }
            } catch (JsonParseException e) {
                LOG.error("Error while parsing Json.", e);
            } catch (IOException e) {
                LOG.error("Error while analyzing sentiment.", e);
            }

        }
    }

    public static void main(String[] args) {

    	// Setup Dataflow options
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create().as(DataflowPipelineOptions.class);
        options.setRunner(DataflowPipelineRunner.class);
        options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
        options.setMaxNumWorkers(3);

        String projectId = options.getProject();

        // Create TableSchemas from their String representation
        TableSchema tweetsTableSchema;
        TableSchema annotatedTweetsTableSchema;
        try {
            tweetsTableSchema = createTableSchema(TWEETS_TABLE_SCHEMA);
            annotatedTweetsTableSchema = createTableSchema(ANNOTATED_TWEETS_TABLE_SCHEMA);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Pipeline p = Pipeline.create(options);

        // Read tweets from Pub/Sub
        PCollection<String> tweets = null;
        tweets = p.apply(PubsubIO.Read.named("Read tweets from PubSub").topic("projects/" + projectId + "/topics/blackfridaytweets"));

        // Format tweets for BigQuery
        PCollection<TableRow> formattedTweets = tweets.apply(ParDo.named("Format tweets for BigQuery").of(new DoFormat()));

        // Create a TableReference for the destination table
        TableReference tableReference = new TableReference();
        tableReference.setProjectId(projectId);
        tableReference.setDatasetId("black_friday_analytics");
        tableReference.setTableId("tweets_raw");

        // Write tweets to BigQuery
        formattedTweets.apply(BigQueryIO.Write.named("Write tweets to BigQuery").to(tableReference).withSchema(tweetsTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).withoutValidation());

        // Filter and annotate tweets with their sentiment from NL API
        // Note: if the pipeline is run as a batch pipeline, the filter condition is inverted
        PCollection<String> filteredTweets = tweets.apply(ParDo.named("Filter and annotate tweets").of(new DoFilterAndProcess()));

        // Format tweets for BigQuery 
        PCollection<TableRow> filteredFormattedTweets = filteredTweets.apply(ParDo.named("Format annotated tweets for BigQuery").of(new DoFormat()));

		// Create a TableReference for the destination table
        TableReference filteredTableReference = new TableReference();
        filteredTableReference.setProjectId(projectId);
        filteredTableReference.setDatasetId("black_friday_analytics");
        filteredTableReference.setTableId("tweets_sentiment");

        // Write tweets to BigQuery
        filteredFormattedTweets.apply(BigQueryIO.Write.named("Write annotated tweets to BigQuery").to(filteredTableReference).withSchema(annotatedTweetsTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();
    }

    /**
     * Connects to the Natural Language API using Application Default Credentials.
     */
    private static CloudNaturalLanguageAPI getLanguageService() throws IOException, GeneralSecurityException {
        final GoogleCredential credential = GoogleCredential.getApplicationDefault().createScoped(CloudNaturalLanguageAPIScopes.all());
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        return new CloudNaturalLanguageAPI.Builder(GoogleNetHttpTransport.newTrustedTransport(), jsonFactory, new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                credential.initialize(request);
            }
        }).setApplicationName(APPLICATION_NAME).build();
    }

    /**
     * Analyzes the sentiment of a text with the Natural Language API
     */
    private static Sentiment analyzeSentiment(CloudNaturalLanguageAPI languageApi, String text) throws IOException {
        AnalyzeSentimentRequest request = new AnalyzeSentimentRequest().setDocument(new Document().setContent(text).setType("PLAIN_TEXT"));
        CloudNaturalLanguageAPI.Documents.AnalyzeSentiment analyze = languageApi.documents().analyzeSentiment(request);

        AnalyzeSentimentResponse response = analyze.execute();
        return response.getDocumentSentiment();
    }

    /**
     * Flattens nested JsonArrays
     */
    private static JsonArray flatten(JsonElement el) {
        JsonArray result = new JsonArray();
        if (el.isJsonArray()) {
            for (JsonElement arrayEl : el.getAsJsonArray()) {
                result.addAll(flatten(arrayEl));
            }
        } else {
            result.add(el);
        }
        return result;
    }

    /**
     * Does some processing on the JSON message describing the tweet
     */
    private static JsonElement cleanup(JsonElement el) {

        SimpleDateFormat dateReader = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        SimpleDateFormat dateWriter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        if (el.isJsonNull()) {
            return el;
        } else if (el.isJsonObject()) {
            Set<String> toBeRemoved = Sets.newHashSet();
            JsonObject objEl = el.getAsJsonObject();
            for (Entry<String, JsonElement> child : objEl.entrySet()) {
                if (child.getValue().isJsonNull()) {
                    toBeRemoved.add(child.getKey());
                } else if (child.getKey().equalsIgnoreCase("created_at")) {
                    // Parse dates like 'Tue Oct 18 07:01:50 +0000 2016' and write them out as "2016-10-18 07:01:50"
                    try {
                        Date date = dateReader.parse(child.getValue().getAsString());
                        objEl.addProperty(child.getKey(), dateWriter.format(date));
                    } catch (ParseException e) {
                        LOG.warn("Error while parsing date", e);
                    }
                } else if (child.getKey().equalsIgnoreCase("coordinates") && child.getValue().isJsonArray()) {
                    objEl.add(child.getKey(), flatten(child.getValue()));
                } else if (child.getKey().equalsIgnoreCase("attributes")) {
                    objEl.addProperty(child.getKey(), child.getValue().toString());
                } else if (child.getValue().isJsonObject()) {
                    objEl.add(child.getKey(), cleanup(child.getValue()));
                } else if (child.getValue().isJsonArray()) {
                    JsonArray newArray = new JsonArray();
                    for (JsonElement arrayEl : child.getValue().getAsJsonArray()) {
                        newArray.add(cleanup(arrayEl));
                    }
                    objEl.add(child.getKey(), newArray);
                }
            }

            // Remove ignored fields
            // TODO: modify schema to include also these fields
            for (String key : IGNORED_FIELDS) {
                if (objEl.has(key)) {
                    objEl.remove(key);
                }
            }
            // Remove fields marked for removal
            // TODO: modify schema to include also these fields
            for (String key : toBeRemoved) {
                objEl.remove(key);
            }

            return objEl;
        } else {
            return el;
        }

    }

    /**
     * Creates a TableRow object from its String JSON representation, using a JsonFactory
     */
    private static TableRow createTableRow(String tweet) throws IOException {
        JsonObject jsonTweet = (JsonObject) cleanup(new JsonParser().parse(tweet).getAsJsonObject());
        return JacksonFactory.getDefaultInstance().fromString(jsonTweet.toString(), TableRow.class);
    }

    /**
     * Creates a TableSchema from its String JSON representation, using a JsonFactory
     */
    private static TableSchema createTableSchema(String schema) throws IOException {
        return JacksonFactory.getDefaultInstance().fromString(schema, TableSchema.class);
    }
}
