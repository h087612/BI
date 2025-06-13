package org.example.bi.Aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.example.bi.Entity.QueryLog;
import org.example.bi.Repository.QueryLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

//@Aspect
//@Component
//public class QueryLogAspect {
//    @Autowired
//    private QueryLogRepository queryLogRepository;
//
//    // 拦截所有 @Repository 或自定义包路径下方法
//    @Around("execution(* org.example.bi.Repository..*(..))")
//    public Object logQueryExecution(ProceedingJoinPoint joinPoint) throws Throwable {
//        long start = System.currentTimeMillis();
//
//        // 执行目标方法
//        Object result = joinPoint.proceed();
//
//        long elapsed = System.currentTimeMillis() - start;
//
//        // 获取方法名与参数（拼SQL参数）
//        String method = joinPoint.getSignature().toShortString();
//        String args = Arrays.stream(joinPoint.getArgs())
//                .map(arg -> arg == null ? "null" : arg.toString())
//                .collect(Collectors.joining(", "));
//
//        String sqlText = "[Method] " + method + " | [Args] " + args + " | [Elapsed] " + elapsed + "ms";
//
//        // 保存日志记录
//        QueryLog log = new QueryLog();
//        log.setSqlText(sqlText);
//        log.setQueryTime(LocalDateTime.now());
//        queryLogRepository.save(log);
//
//        return result;
//    }
//}
