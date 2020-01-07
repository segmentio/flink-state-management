package com.segment.json

import java.time.Instant

import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JNull, JString}

case object InstantSerializer extends CustomSerializer[Instant](_ => (
  {
    case JString(s) => Instant.parse(s)
    case JNull => null
  },
  {
    case i: Instant => JString(i.toString)
  }
))
