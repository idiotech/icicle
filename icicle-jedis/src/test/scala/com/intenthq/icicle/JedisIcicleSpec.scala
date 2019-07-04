package com.intenthq.icicle

import java.util
import java.util.Optional

import _root_.redis.clients.jedis.exceptions.JedisDataException
import _root_.redis.clients.jedis.JedisCluster
import com.intenthq.icicle.redis.IcicleRedisResponse
import org.specs2.matcher.ThrownExpectations
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.specification.Scope

object JedisIcicleSpec extends Specification  {
  val luaScript = "foo"
  val sha = "abcdef1234567890"

  "constructor" should {
    "throw a InvalidServerFormatException exception if the host and string passed is invalid" in {
      new JedisIcicle("foibles", "1234") must throwA[InvalidServerFormatException]
    }
  }

  "#loadLuaScript" should {
    "call to redis to load the script" in new Context {
      underTest.loadLuaScript(luaScript, 1)

      there was one(jedis).scriptLoad(luaScript, "{1}")
    }

    "returns the SHA returned by the call to Redis" in new Context {
      jedis.scriptLoad(luaScript, "{1}") returns sha

      underTest.loadLuaScript(luaScript, 1) must_== sha
    }

    "returns the resource if the call was successful" in new Context {
      underTest.loadLuaScript("foo", 1)

      there was one(jedis).scriptLoad("foo", "{1}")
    }

    "rethrows any exception the call throws" in new Context {
      val testScript = "foo"
      jedis.scriptLoad(testScript, "{1}") throws new RuntimeException

      underTest.loadLuaScript(testScript, 1) must throwA[RuntimeException]
    }
  }

  "#evalLuaScript" should {
    val args = util.Arrays.asList("foo")
    val keys = util.Arrays.asList("bar")
    val keysAndArgs = util.Arrays.asList("foo", "bar")
    val response: java.util.List[java.lang.Long] = util.Arrays.asList(12L, 34L, 56L, 78L, 1L)
    val argsAsArray: Array[String] = keysAndArgs.toArray(Array[String]())

    "call to redis to eval the script with the given args" in new Context {

      jedis.evalsha(any, any[util.List[String]], any[util.List[String]]) returns response

      underTest.evalLuaScript(sha, keys, args)

      there was one(jedis).evalsha(sha, keys, args)
    }

    "returns the response returned by the call to Redis wrapped up as an IcicleRedisResponse" in new Context {
      jedis.evalsha(any, any[util.List[String]], any[util.List[String]]) returns response

      underTest.evalLuaScript(sha, keys, args) must_== Optional.of(new IcicleRedisResponse(response))
    }

    "returns absent when a JedisDataException is thrown" in new Context {
      jedis.evalsha(any, any[util.List[String]], any[util.List[String]]) throws new JedisDataException("foo", new Throwable)

      underTest.evalLuaScript(sha, keys, args).isPresent must beFalse
    }

    "rethrows any exception the call throws" in new Context {
      val testScript = "foo"
      jedis.scriptLoad(testScript, "{1}") throws new RuntimeException

      underTest.loadLuaScript(testScript, 1) must throwA[RuntimeException]
    }
  }

  trait Context extends Scope with Mockito with ThrownExpectations {
    val jedis = mock[JedisCluster]
    val underTest = new JedisIcicle(jedis)
  }
}
