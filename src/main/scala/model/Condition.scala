package model

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}

case class Condition(
                 @JsonProperty("attributeIdx") var attributeIdx: Int,
                 @JsonProperty("relation") var relation: String,
                 @JsonProperty("value") var value: Double)
