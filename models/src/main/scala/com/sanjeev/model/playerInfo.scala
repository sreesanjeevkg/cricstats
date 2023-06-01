package com.sanjeev.model

case class playerInfo(
                       player_identifier: String,
                       Name: Option[String],
                       Role: Option[String],
                       BattingStyle: Option[String],
                       BowlingStyle: Option[String],
                       FieldingPosition: Option[String]
                     )
