package com.malife.demo.core.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.exceptions.JedisConnectionException;

@Component
public class Store {

    private static final Logger log = LoggerFactory.getLogger(Store.class);

    private final Map<String, String> keyMap = Map.of(
        "int:pub:auth", "http://int.apigw.com",
        "int:prv:auth", "http://int.apigw-prv.com",
        "srv:pub:auth", "http://srv.apigw.com",
        "srv:prv:auth", "http://srv.apigw-prv.com"
    );

    // 스레드 안정을 위한 ConcurrentHashMap
    private final Map<String, String> tokens = new ConcurrentHashMap<>();

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    // 5개의 포트를 콤마(,) 구분자로 전달받음
    @Value("${spring.data.redis.ports:6379,6380,6381,6382,6383}")
    private String redisPortsConfig;

    @Value("${spring.data.redis.username:}")
    private String redisUsername;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Value("${spring.data.redis.timeout:2000}")
    private int timeout;

    private List<Integer> redisPorts;
    
    // Jedis 7.5.2 권장: 포트별 RedisClient 관리
    private final Map<Integer, RedisClient> clientMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        this.redisPorts = Arrays.stream(redisPortsConfig.split(","))
                .filter(s -> s.matches("^\\s*[0-9]+\\s*$"))
                .map(s -> Integer.parseInt(s.trim()))
                .toList();

        // 초기화 시 매핑된 키의 모든 토큰 로드
        refreshAllTokens();
    }

    /**
     * URL에 해당하는 캐싱된 토큰 반환
     */
    public String getToken(String url) {
        return this.tokens.get(url);
    }

    /**
     * [스케줄러 호출용] keyMap에 등록된 모든 Redis Key의 토큰을 최신 상태로 갱신
     */
    public void refreshAllTokens() {
        log.info("Starting scheduled Redis token refresh process...");
        int successCount = 0;

        for (String key : keyMap.keySet()) {
            boolean updated = refreshTokenByKey(key);
            if (updated) {
                successCount++;
            }
        }

        log.info("Redis token refresh completed. (Success: {}/{})", successCount, keyMap.size());
    }

    /**
     * 단일 Redis Key에 해당하는 토큰을 조회하여 local 메모리(tokens)에 업데이트
     */
    public boolean refreshTokenByKey(String key) {
        String url = this.keyMap.get(key);
        if (url == null) {
            log.warn("keyMap에 매핑되지 않은 키입니다: {}", key);
            return false;
        }

        try {
            // 5개 포트를 순회하며 성공할 때까지 execution 시도
            String token = executeWithFailover(client -> client.get(key));

            if (token != null) {
                this.tokens.put(url, token);
                log.info("토큰 동기화 성공 [Key: {}, URL: {}]", key, url);
                return true;
            } else {
                log.warn("Redis에 해당 키의 토큰이 존재하지 않습니다: {}", key);
                return false;
            }
        } catch (Exception e) {
            log.error("Redis 토큰 조회 실패 (Key: {})", key, e);
            return false;
        }
    }

    /**
     * 5개 포트를 순차적으로 시도하여 Redis 명령을 실행하는 Failover 로직
     */
    private <T> T executeWithFailover(RedisCommand<T> command) {
        for (int port : redisPorts) {
            try {
                RedisClient client = getOrCreateClient(port);
                return command.execute(client);
            } catch (JedisConnectionException e) {
                log.warn("Redis 접속 실패 [Host: {}, Port: {}] - 다음 포트로 재시도합니다. (Error: {})", 
                        redisHost, port, e.getMessage());
            } catch (Exception e) {
                log.error("Redis 작업 처리 중 오류 발생 [Port: {}]", port, e);
            }
        }
        throw new RuntimeException("모든 지정된 포트(" + redisPorts + ")로의 Redis 접속에 실패했습니다.");
    }

    /**
     * Jedis 7.5.2 공식 규격에 맞춰 RedisClient 생성 및 캐싱
     */
    private synchronized RedisClient getOrCreateClient(int port) {
        return clientMap.computeIfAbsent(port, p -> {
            // 1. 인증 및 타임아웃 설정 (DefaultJedisClientConfig)
            DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                    .timeoutMillis(timeout);

            if (redisUsername != null && !redisUsername.isBlank()) {
                configBuilder.user(redisUsername);
            }
            if (redisPassword != null && !redisPassword.isBlank()) {
                configBuilder.password(redisPassword);
            }

            // 2. 풀 커넥션 설정 (ConnectionPoolConfig)
            ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
            poolConfig.setMaxTotal(10);
            poolConfig.setMaxIdle(5);
            poolConfig.setMinIdle(1);

            // 3. RedisClient 빌드 (7.5.2 매뉴얼 표준)
            HostAndPort hostAndPort = new HostAndPort(redisHost, p);
            log.info("Creating RedisClient for {}:{}", redisHost, p);

            return RedisClient.builder()
                    .hostAndPort(hostAndPort)
                    .clientConfig(configBuilder.build())
                    .poolConfig(poolConfig)
                    .build();
        });
    }

    @FunctionalInterface
    private interface RedisCommand<T> {
        T execute(RedisClient client);
    }

    @PreDestroy
    public void close() {
        clientMap.forEach((port, client) -> {
            if (client != null) {
                try {
                    client.close();
                    log.info("RedisClient closed for port: {}", port);
                } catch (Exception e) {
                    log.error("Error closing RedisClient for port: {}", port, e);
                }
            }
        });
        clientMap.clear();
    }
}
