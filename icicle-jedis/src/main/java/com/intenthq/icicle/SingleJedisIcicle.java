package com.intenthq.icicle;

import com.intenthq.icicle.redis.Redis;
import com.intenthq.icicle.redis.IcicleRedisResponse;

import java.util.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Implementation of the Icicle Redis interface for Jedis.
 */
public class SingleJedisIcicle implements Redis {

    private final Jedis jedis;

    public SingleJedisIcicle(final String host, final int port, final String password) {
        Jedis jedis = new Jedis(host, port);
        if (password != null) jedis.auth(password);
        this.jedis = jedis;
    }

    /**
     * Load the given Lua script into Redis.
     *
     * @param luaScript The Lua script to load into Redis.
     * @return The SHA of the loaded Lua script.
     */
    @Override
    public String loadLuaScript(final String luaScript, int partition) {
        return jedis.scriptLoad(luaScript);
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
            List<Long> results = (List<Long>) jedis.evalsha(luaScriptSha, keys, arguments);
            return Optional.of(new IcicleRedisResponse(results));
        } catch (JedisDataException e) {
            return Optional.empty();
        }
    }
}
