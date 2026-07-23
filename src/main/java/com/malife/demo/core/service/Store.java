package com.malife.demo.core.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.RedisClient;

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

    private RedisClient redisClient;

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @PostConstruct
    public void init() {
        // Jedis 7.5.2 규격: HostAndPort 객체를 통한 RedisClient 생성
        HostAndPort endpoint = new HostAndPort(redisHost, redisPort);
        this.redisClient = RedisClient.builder()
                .hostAndPort(endpoint)
                .build();

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
     * 
     * @param key Redis Key (예: "int:pub:auth")
     * @return 갱신 성공 여부
     */
    public boolean refreshTokenByKey(String key) {
        String url = this.keyMap.get(key);
        if (url == null) {
            log.warn("keyMap에 매핑되지 않은 키입니다: {}", key);
            return false;
        }

        try {
            String token = this.redisClient.get(key);

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

    @PreDestroy
    public void close() {
        if (this.redisClient != null) {
            this.redisClient.close();
            log.info("RedisClient 접속 자원 해제 완료");
        }
    }
}
