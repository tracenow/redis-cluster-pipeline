package cn.trace.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.PipelineBase;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

/**
 * @author trace
 * 
 */
public class RedisCluster extends JedisCluster {

    private static final Logger logger = LoggerFactory.getLogger(RedisCluster.class);
    
    private static final int DEFAULT_TIMEOUT = 10 * 1000;
    
    private final Map<String, JedisPool> nodeMap;
    
    private final TreeMap<Long, String> slotHostMap;
    
    public RedisCluster(Set<HostAndPort> clusterNodes, JedisPoolConfig jedisPoolConfig) {
        this(clusterNodes, DEFAULT_TIMEOUT, jedisPoolConfig);
    }
    
    public RedisCluster(Set<HostAndPort> clusterNodes, int timeout, JedisPoolConfig jedisPoolConfig) {
        super(clusterNodes, timeout, jedisPoolConfig);
        nodeMap = this.getClusterNodes();
        slotHostMap = getSlotHostMap(nodeMap.keySet().iterator().next());
    }

    public RedisClusterPipeline pipelined() {
        return new RedisClusterPipeline();
    }
    
    @SuppressWarnings("unchecked")
    private TreeMap<Long, String> getSlotHostMap(String anyHostAndPortStr) {
        TreeMap<Long, String> tree = new TreeMap<Long, String>();
        String parts[] = anyHostAndPortStr.split(":");
        HostAndPort anyHostAndPort = new HostAndPort(parts[0], Integer.parseInt(parts[1]));
        try {
            Jedis jedis = new Jedis(anyHostAndPort.getHost(), anyHostAndPort.getPort());
            List<Object> clusterSlots = jedis.clusterSlots();
            for (Object clusterSlot : clusterSlots) {
                List<Object> list = (List<Object>) clusterSlot;
                List<Object> master = (List<Object>) list.get(2);
                String hostAndPort = new String((byte[]) master.get(0)) + ":" + master.get(1);
                tree.put((Long) list.get(0), hostAndPort);
                tree.put((Long) list.get(1), hostAndPort);
            }
            jedis.close();
        } catch (Exception e) {
            logger.error("get slot host map failed : " + anyHostAndPortStr, e);
        }
        return tree;
    }

    public class RedisClusterPipeline extends PipelineBase implements Closeable {

        private final Map<String, Jedis> slotJedisMap = new HashMap<String, Jedis>();

        @Override
        protected Client getClient(String key) {
            return getClient(SafeEncoder.encode(key));
        }

        @Override
        protected Client getClient(byte[] key) {
            Integer slot = JedisClusterCRC16.getSlot(key);
            Map.Entry<Long, String> entry = slotHostMap.lowerEntry(Long.valueOf(slot + 1));
            Jedis jedis = slotJedisMap.get(entry.getValue());
            if (jedis == null) {
                JedisPool jedisPool = nodeMap.get(entry.getValue());
                jedis = jedisPool.getResource();
                slotJedisMap.put(entry.getValue(), jedis);
            }
            return jedis.getClient();
        }

        public void sync() {
            if (getPipelinedResponseLength() > 0) {
                for (Jedis jedis : slotJedisMap.values()) {
                    for(Object obj : jedis.getClient().getAll()) {
                        generateResponse(obj);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            for (Jedis jedis : slotJedisMap.values()) {
                jedis.close();
            }
            slotJedisMap.clear();
        }
    }
}
