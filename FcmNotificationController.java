package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.NotificationMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping(value = "/fcm")
public class FcmNotificationController {

    private final NotificationMgmt notificationMgmt;

    public FcmNotificationController(NotificationMgmt notificationMgmt) {
        this.notificationMgmt = notificationMgmt;
    }

    @PostMapping(value = "/send")
    public SuccessResponse sendNotification(@RequestHeader("authToken") String authToken,
                                            @RequestBody NotificationRequest request) {

        SuccessResponse response = new SuccessResponse();

        try {
            response = notificationMgmt.sendNotification(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Send Notification", e);
        }

        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @GetMapping(value = "/list")
    public ListNotificationResponse listUserNotifications(@RequestHeader("authToken") String authToken) {

        ListNotificationResponse response = new ListNotificationResponse();

        try {
            response = notificationMgmt.listUserNotifications(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List User Notifications", e);
        }

        return response;
    }

    // 2/11/2021 release (4.0.11)
    @GetMapping(value = "/list/v1")
    public ListNotificationResponse listUserNotificationsV1(@RequestHeader("authToken") String authToken,
                                                            @RequestParam("page") int page) {

        ListNotificationResponse response = new ListNotificationResponse();

        try {
            response = notificationMgmt.listUserNotificationsV1(ServiceUtils.createAuthObj(authToken), page);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List User Notifications", e);
        }

        return response;
    }

    @PutMapping(value = "/update")
    public SuccessResponse updateNotification(@RequestHeader("authToken") String authToken,
                                              @RequestBody UpdateNotificationRequest request) {

        SuccessResponse response = new SuccessResponse();

        try {
            response = notificationMgmt.updateNotification(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Notification", e);
        }

        return response;
    }

    @DeleteMapping(value = "/delete/{notificationId}")
    public SuccessResponse deleteNotificationById(@RequestHeader("authToken") String authToken,
                                                  @PathVariable("notificationId") long notificationId) {

        SuccessResponse response = new SuccessResponse();

        try {
            response = notificationMgmt.deleteNotification(ServiceUtils.createAuthObj(authToken), notificationId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Delete Notification", e);
        }

        return response;
    }

    @DeleteMapping(value = "/remove")
    public SuccessResponse removeNotifications() {

        SuccessResponse response = new SuccessResponse();

        try {
            response = notificationMgmt.removeNotifications();
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Remove Notification", e);
        }

        return response;
    }

    @PostMapping(value = "/custom")
    public SuccessResponse customNotification(@RequestParam String password, @RequestBody CustomNotificationRequest request) {
        SuccessResponse response = new SuccessResponse();
        try {
            response = notificationMgmt.sendCustomNotification(password, request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Custom Notification", e);
        }
        return response;
    }
}
