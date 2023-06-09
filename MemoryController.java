package com.qaddoo.controller;

import com.qaddoo.pojo.entity.HeapMemoryStatsResponse;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/memory")
public class MemoryController {

    @GetMapping(value = "heapStats")
    public HeapMemoryStatsResponse getHeapMemoryStats(HttpServletRequest request) {
        HeapMemoryStatsResponse response = new HeapMemoryStatsResponse();
        if (!request.getHeader("passKey").equalsIgnoreCase(ServiceUtils.PASSWORD)) {
            return ServiceUtils.setResponse(response, false, "Get Heap Memory Stats", new Exception("Not Authorized"));
        }
        response.setHeapSize(Runtime.getRuntime().totalMemory());
        response.setMaxHeapSize(Runtime.getRuntime().maxMemory());
        response.setFreeHeapSize(Runtime.getRuntime().freeMemory());

        return ServiceUtils.setResponse(response, true, "Get Heap Memory Stats");
    }
}
