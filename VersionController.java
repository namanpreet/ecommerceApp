package com.qaddoo.controller;

import com.qaddoo.pojo.entity.AppUpdateResponse;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/app")
public class VersionController {

    private static final int FLEXIBLE = 0;
    private static final int IMMEDIATE = 1;

    // 20/09/2021 (4.0.9) and previous releases
    @GetMapping(value = "/update")
    public AppUpdateResponse getUpdateInfo(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 12/10/2021 release (4.0.10)
    @GetMapping(value = "/update/v1")
    public AppUpdateResponse getUpdateInfoV1(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 02/11/2021 (4.0.11) and 09/11/2021 (4.0.12) release
    @GetMapping(value = "/update/v2")
    public AppUpdateResponse getUpdateInfoV2(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 26/11/2021 release (4.0.13)
    @GetMapping(value = "/update/v3")
    public AppUpdateResponse getUpdateInfoV3(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 14/12/2021 (4.0.14) and 30/12/2021 (4.0.15) release
    @GetMapping(value = "/update/v4")
    public AppUpdateResponse getUpdateInfoV4(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 01/02/2022 (4.0.16) release (Force Upgrade)
    @GetMapping(value = "/update/v5")
    public AppUpdateResponse getUpdateInfoV5(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 10/02/2022 (4.0.17) and 22/02/2022 (4.0.18) release
    @GetMapping(value = "/update/v6")
    public AppUpdateResponse getUpdateInfoV6(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 11/03/2022 (4.0.19) release (Force Upgrade)
    @GetMapping(value = "/update/v7")
    public AppUpdateResponse getUpdateInfoV7(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 05/04/2022 (4.0.20) release (Force Upgrade)
    @GetMapping(value = "/update/v8")
    public AppUpdateResponse getUpdateInfoV8(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 08/04/2022 (4.0.21) (Force Upgrade) and 27/04/2022 (4.0.22) release
    @GetMapping(value = "/update/v9")
    public AppUpdateResponse getUpdateInfoV9(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 05/05/2022 (4.0.23) release (Force Upgrade)
    @GetMapping(value = "/update/v10")
    public AppUpdateResponse getUpdateInfoV10(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 13/06/2022 (4.0.24) release (Force Upgrade)
    @GetMapping(value = "/update/v11")
    public AppUpdateResponse getUpdateInfoV11(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 22/06/2022 (4.0.25) release (Force Upgrade)
    @GetMapping(value = "/update/v12")
    public AppUpdateResponse getUpdateInfoV12(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 05/07/2022 (4.0.26) release (Force Upgrade)
    @GetMapping(value = "/update/v13")
    public AppUpdateResponse getUpdateInfoV13(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }

    // 02/08/2022 (4.0.27) release (Force Upgrade)
    @GetMapping(value = "/update/v14")
    public AppUpdateResponse getUpdateInfoV14(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(IMMEDIATE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }


    // 05/09/2022 (4.0.28 ) release (Force Upgrade)
    @GetMapping(value = "/update/v15")
    public AppUpdateResponse getUpdateInfoV15(@RequestHeader("authToken") String authToken) {

        AppUpdateResponse response = new AppUpdateResponse();
        response.setUpdate(FLEXIBLE);
        return ServiceUtils.setResponse(response, true, "Update Type");
    }
}
