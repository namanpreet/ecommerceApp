package com.qaddoo.controller;

import com.qaddoo.pojo.entity.FileUploadResponse;
import com.qaddoo.pojo.entity.GetVideoResponse;
import com.qaddoo.service.manager.FileUploadMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

@RestController
@RequestMapping(value = "/file")
public class FileUploadController {

    private final FileUploadMgmt fileUploadMgmt;

    public FileUploadController(FileUploadMgmt fileUploadMgmt) {
        this.fileUploadMgmt = fileUploadMgmt;
    }

    @PostMapping(value = "/upload")
    public FileUploadResponse uploadFile(@RequestHeader("authToken") String authToken,
                                         @RequestBody List<MultipartFile> files) {

        FileUploadResponse response = new FileUploadResponse();

        try {
            response = fileUploadMgmt.uploadFile(ServiceUtils.createAuthObj(authToken), files);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, true, "Upload Image", e);
        }

        return response;
    }

    // 30/12/2021 (4.0.15) and previous versions
    @Deprecated
    @GetMapping(value = "/get/{filename}")
    public void getFile(@RequestHeader("authToken") String authToken,
                        @PathVariable("filename") String fileName, HttpServletResponse response) {

        try {
            fileUploadMgmt.retrieveImage(ServiceUtils.createAuthObj(authToken), fileName, response);
        } catch (Exception e) {
            e.printStackTrace();
            response.setHeader("Exception", e.getCause().getMessage());
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    // 30/12/2021 (4.0.15) and previous versions
    @Deprecated
    @GetMapping(value = "/get/video/{filename}")
    public GetVideoResponse getVideo(@RequestHeader("authToken") String authToken, @PathVariable("filename") String filename) {

        GetVideoResponse response = new GetVideoResponse();

        try {
            response = fileUploadMgmt.retrieveVideo(ServiceUtils.createAuthObj(authToken), filename);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Video", e);
        }

        return response;
    }

    // not in use
    @Deprecated
    @GetMapping(value = "/get/privacy")
    public GetVideoResponse getPrivacyPolicy(@RequestHeader("authToken") String authToken) {

        GetVideoResponse response = new GetVideoResponse();

        try {
            response = fileUploadMgmt.getPrivacyPolicy(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Privacy Policy", e);
        }

        return response;
    }

    // not in use
    @Deprecated
    @GetMapping(value = "/get/terms")
    public GetVideoResponse getTermsConditions(@RequestHeader("authToken") String authToken) {

        GetVideoResponse response = new GetVideoResponse();

        try {
            response = fileUploadMgmt.getTermsConditions(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get Terms and Conditions", e);
        }

        return response;
    }
}
