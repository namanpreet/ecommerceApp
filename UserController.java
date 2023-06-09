package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.UserMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(value = "/user")
public class UserController {

    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    private final UserMgmt userManager;

    public UserController(UserMgmt userManager) {
        this.userManager = userManager;
    }

    @PutMapping(value = "/emailOtp")
    public SendOtpResponse emailOtp(@RequestBody OtpRequest request, HttpServletRequest servletRequest) {
        logger.info("Request on endpoint /user/emailOtp = {}", request);
        SendOtpResponse response = new SendOtpResponse();
        String userAgent = servletRequest.getHeader("user-agent");

        logger.info("User-agent - " + userAgent);

        try {
            response = userManager.sendEmailOtp(request, userAgent);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Send Email OTP", e);
        }

        logger.info("Exiting /user/emailOtp with response = {}", response);
        return response;
    }

    @PutMapping(value = "/emailLogin")
    public LoginResponse emailLogin(@RequestBody LoginRequest request) {
        logger.info("Request on endpoint /user/emailLogin = {}", request);
        LoginResponse response = new LoginResponse();

        try {
            response = userManager.emailLogin(request);
        } catch (Exception e) {
            logger.warn("Exception in logging user----" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Login User", e);
        }

        logger.info("Exiting /user/emailLogin with response = {}", response);
        return response;
    }

    @PutMapping(value = "/sendOtp")
    public SendOtpResponse phoneOtp(@RequestBody OtpRequest request, HttpServletRequest servletRequest) {
        logger.info("Request on endpoint /user/phoneOtp = {}", request);
        SendOtpResponse response = new SendOtpResponse();
        String userAgent = servletRequest.getHeader("user-agent");
        String appAuthKey = servletRequest.getHeader("appAuthKey");
        String deviceInstallationId = servletRequest.getHeader("deviceInstallationId");
        String ipAddress = servletRequest.getRemoteAddr();

        logger.info("User-agent - " + userAgent);
        logger.info("IP Address - " + ipAddress);
        logger.info("appAuthKey - " + appAuthKey);

        try {
            response = userManager.sendPhoneOtp(request, ipAddress, userAgent, appAuthKey , deviceInstallationId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Send OTP", e);
        }

        logger.info("Exiting /user/phoneOtp with response = {}", response);
        return response;
    }

    @PutMapping(value = "/phoneLogin")
    public LoginResponse phoneLogin(@RequestBody PhoneLoginRequest request, HttpServletRequest servletRequest) {
        logger.info("Request on endpoint /user/phoneLogin = {}", request);
        LoginResponse response = new LoginResponse();
        String deviceInstallationId = servletRequest.getHeader("deviceInstallationId");


        try {
            response = userManager.phoneLogin(request, servletRequest.getRemoteAddr() , deviceInstallationId);
        } catch (Exception e) {
            logger.warn("Exception in logging user----" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Login User", e);
        }

        logger.info("Exiting /user/phoneLogin with response = {}", response);
        return response;
    }

    @GetMapping(value = "/logout")
    public LogoutResponse logout(@RequestHeader("authToken") String authToken) {

        LogoutResponse response = new LogoutResponse();

        try {
            response = userManager.logout(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "User Logout", e);
        }

        return response;
    }

    @PutMapping(value = "/verifyEmail")
    public SendOtpResponse verifyEmail(@RequestHeader("authToken") String authToken,
                                       @RequestBody OtpRequest request) {

        SendOtpResponse response = new SendOtpResponse();

        try {
            response = userManager.verifyEmail(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Verify Email", e);
        }

        return response;
    }

    @PutMapping(value = "/verifyPhone")
    public SendOtpResponse verifyPhone(@RequestHeader("authToken") String authToken,
                                       @RequestBody OtpRequest request) {

        SendOtpResponse response = new SendOtpResponse();

        try {
            response = userManager.verifyPhone(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Verify Phone", e);
        }

        return response;
    }

    @PutMapping(value = "/update")
    public GetUserDetailResponse updateUser(@RequestHeader("authToken") String authToken,
                                            @RequestBody UpdateUserRequest request) {
        logger.debug("UserController.updateUser---Enter--" + request);
        GetUserDetailResponse response = new GetUserDetailResponse();
        try {
            response = userManager.updateUser(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update User", e);
        }
        logger.debug("UserController.updateUser---Exit---");
        return response;
    }

    @GetMapping(value = "/detail")
    public GetUserDetailResponse getUserDetail(@RequestHeader("authToken") String authToken,
                                               @RequestParam(value = "redirectInfo", required = false) boolean redirectInfo) {
        GetUserDetailResponse response = new GetUserDetailResponse();
        logger.debug("UserController.getUserDetail-----Enter---");
        try {
            response = userManager.getUserDetails(ServiceUtils.createAuthObj(authToken), redirectInfo);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Get User Details failed", e);
        }
        logger.debug("UserController.getUserDetail---Exit---");
        return response;
    }

    // list Active Users
    @PutMapping(value = "/list")
    public ListUserResponse listUsers(@RequestHeader("authToken") String authToken,
                                      @RequestBody ListUserRequest request) {
        logger.debug("UserController.listUsers----Enter-----" + request);
        ListUserResponse response = new ListUserResponse();
        try {
            response = userManager.listNearbyUsers(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            logger.warn("UserController.listUsers----Exception in fetching list of Users---" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "List Active Users", e);
        }

        logger.debug("UserController.listUsers----Exit--");
        return response;
    }

    @GetMapping(value = "/list/{tanId}")
    public ListUserResponse listUsersInTan(@RequestHeader("authToken") String authToken,
                                           @PathVariable("tanId") long tanId) {

        ListUserResponse response = new ListUserResponse();

        try {
            response = userManager.listUsersInTan(ServiceUtils.createAuthObj(authToken), tanId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Users in Group", e);
        }

        return response;

    }

    @PostMapping(value = "/block/{handleName}")
    public BlockUserResponse blockUser(@RequestHeader("authToken") String authToken,
                                       @PathVariable("handleName") String handleName) {

        BlockUserResponse response = new BlockUserResponse();

        try {
            response = userManager.blockUser(ServiceUtils.createAuthObj(authToken), handleName.trim());
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Block User", e);
        }

        return response;
    }

    @PostMapping(value = "/unblock/{blockedId}")
    public BlockUserResponse unblockUser(@RequestHeader("authToken") String authToken,
                                         @PathVariable("blockedId") long blockedId) {

        BlockUserResponse response = new BlockUserResponse();

        try {
            response = userManager.unblockUser(ServiceUtils.createAuthObj(authToken), blockedId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Unblock User", e);
        }

        return response;
    }


    @GetMapping(value = "/block/list")
    public ListUserResponse getBlockedUsersList(@RequestHeader("authToken") String authToken) {

        ListUserResponse response = new ListUserResponse();

        try {
            response = userManager.getBlockedUserList(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Blocked Users List", e);
        }

        return response;
    }

    // 14/12/2021 release (4.0.14)
    @PutMapping(value = "/exist")
    public ExistingUsersResponse existingUsers(@RequestHeader("authToken") String authToken, @RequestBody ExistingUsersRequest request) {

        ExistingUsersResponse response = new ExistingUsersResponse();

        try {
            response = userManager.existingUsers(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Find Existing Users", e);
        }

        return response;
    }

    // 30/12/2021 release (4.0.15)
    @PutMapping(value = "/feedBack")
    public FeedBackResponse userFeedBack(@RequestHeader(value = "authToken", required = false) String authToken, @RequestBody FeedBackRequest request) {

        FeedBackResponse response = new FeedBackResponse();

        try {
            response = userManager.userFeedBack(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add FeedBack", e);
        }
        return response;
    }

}
