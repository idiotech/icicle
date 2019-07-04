package com.intenthq.icicle;

import com.intenthq.icicle.redis.Redis;
import com.intenthq.icicle.redis.IcicleRedisResponse;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Implementation of the Icicle Redis interface for Jedis.
 */
public class JedisIcicle implements Redis {
  private static final Pattern SERVER_FORMAT = Pattern.compile("^([^:]+):([0-9]+)$");

  private final JedisCluster jedisCluster;


  /**
   * Create an instance of JedisIcicle from a host and port string of the format "server:port".
   *
   * @param hostAndPort A host and port string for a Redis instance to use for ID generation, of the format "host:port".
   */
  public JedisIcicle(final String hostAndPort, final String password) {
    this.jedisCluster = jedisClusterFromServerAndPort(hostAndPort, password);
  }

  public JedisIcicle(final String hosts, final int port, final String password) {
    HashSet<HostAndPort> nodeSet = new HashSet<>();
    for (String host: hosts.split(",")) {
      nodeSet.add(new HostAndPort(host, port));
    }
    this.jedisCluster = getAuthed(nodeSet, password);
  }
  /**
   * Create an instance of JedisIcicle from an existing JedisPool instance.
   *
   * @param jedisCluster An existing JedisPool instance you have configured that can be used for the ID generation.
   */
  public JedisIcicle(final JedisCluster jedisCluster) {
    this.jedisCluster = jedisCluster;
  }

  /**
   * Getter for the JedisPool instance used for the ID generation.
   *
   * @return The instance of JedisPool that was either passed or created from the host and port given.
   */
  public JedisCluster getJedisCluster() {
    return jedisCluster;
  }

  /**
   * Load the given Lua script into Redis.
   *
   * @param luaScript The Lua script to load into Redis.
   * @return The SHA of the loaded Lua script.
   */
  @Override
  public String loadLuaScript(final String luaScript, int partition) {
    return jedisCluster.scriptLoad(luaScript, "{" + partition + "}");
  }

  /**
   * Execute the Lua script with the given SHA, passing the given list of arguments.
   *
   * @param luaScriptSha The SHA of the Lua script to execute.
   * @param arguments The arguments to pass to the Lua script.
   * @return The optional result of executing the Lua script. Absent if the Lua script referenced by the SHA was missing
   * when it was attempted to be executed.
   */
  @Override
  public Optional<IcicleRedisResponse> evalLuaScript(final String luaScriptSha, final List<String> keys, final List<String> arguments) {
    try {
      @SuppressWarnings("unchecked")
      List<Long> results = (List<Long>) jedisCluster.evalsha(luaScriptSha, keys, arguments);
      return Optional.of(new IcicleRedisResponse(results));
    } catch (JedisDataException e) {
      return Optional.empty();
    }
  }

  /**
   * Given a string of the format "host:port", create a new JedisPool instance or throw a InvalidServerFormatException
   * if invalid.
   *
   * @param hostAndPort A host and port string for a Redis instance to use for ID generation, of the format "host:port".
   * @return A JedisPool instance pointing at the given host and port.
   * @throws InvalidServerFormatException If the given parameter is not of the format "host:port".
   */
  private JedisCluster jedisClusterFromServerAndPort(final String hostAndPort, final String password) {

    List<String> nodes = Arrays.asList(hostAndPort.split(","));
    HashSet<HostAndPort> nodeSet = new HashSet<>();
    for (String s: nodes) {
      Matcher matcher = SERVER_FORMAT.matcher(s);
      if (!matcher.matches()) {
        throw new InvalidServerFormatException(s);
      } else {
        nodeSet.add(new HostAndPort(matcher.group(1), Integer.valueOf(matcher.group(2))));
      }
    }

    return getAuthed(nodeSet, password);
  }

  private JedisCluster getAuthed(Set<HostAndPort> nodeSet, final String password) {
    return new JedisCluster(
            nodeSet,
            2000,
            2000,
            5,
            password,
            new GenericObjectPoolConfig()
    );

  }
}
