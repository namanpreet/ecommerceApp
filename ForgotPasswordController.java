//package com.qaddoo.controller;
//
//import com.google.common.base.Strings;
//import com.qaddoo.pojo.ControllerServiceResponse;
//import com.qaddoo.pojo.entity.ForgotPwdRequest;
//import com.qaddoo.pojo.entity.ForgotPwdResponse;
//import com.qaddoo.pojo.entity.GetUserDetailResponse;
//import com.qaddoo.service.exception.ValidationException;
//import com.qaddoo.service.manager.ForgotPwdMgmt;
//import com.qaddoo.service.util.ServiceUtils;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.*;
//
//import javax.mail.Address;
//import javax.mail.Message;
//import javax.mail.Session;
//import javax.mail.Transport;
//import javax.mail.internet.InternetAddress;
//import javax.mail.internet.MimeMessage;
//import javax.servlet.http.HttpServletRequest;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//
//@Controller
//@RequestMapping(value = "/reset")
//public class ForgotPasswordController {
//
//    private final ForgotPwdMgmt forgotPwdManager;
//    // private final Logger log = Logger.getLogger(ForgotPasswordController.class);
//    private static final String mail_smtp_host_ip = "localhost";
//    private static final String mail_smtp_port_no = "25";
//    private static final String mail_smtp_auth = "mail.smtp.auth";
//    private static final String mail_smtp_starttls_enable = "mail.smtp.starttls.enable";
//    private static final String mail_smtp_host = "mail.smtp.host";
//    private static final String mail_smtp_port = "mail.smtp.port";
//    private static final String mail_smtp_ssl_trust = "mail.smtp.ssl.trust";
//    private static final String mail_debug = "mail.debug";
//    private static final String subject = "Qaddoo password reset";
//    private static final String qaddooProd = "http://platform.qaddoo.com:8080/qaddoo-restful-service/openresetpage.jsp?token=";
//    private static final String qaddoo = "http://localhost:8080/openresetpage.jsp?token=";
//
//    public ForgotPasswordController(ForgotPwdMgmt forgotPwdManager) {
//        this.forgotPwdManager = forgotPwdManager;
//    }
//
//    // Process form submission from forgotPassword page
//    @RequestMapping(value = "/email", method = RequestMethod.GET)
//    public @ResponseBody
//    ControllerServiceResponse sendEmail(@RequestParam("email") String userEmail,
//                                        HttpServletRequest request) {
//        ForgotPwdResponse usrResponse = new ForgotPwdResponse();
//        GetUserDetailResponse dtResponse = new GetUserDetailResponse();
//        try {
//            //	log.info("Mail service starts !!!");
//            String to = request.getParameter("email");
//
//            try {
//                usrResponse = forgotPwdManager.getDetailsByUser(to);
//                // Update token
//                if (!usrResponse.isValid()) {
//
//                    ForgotPwdResponse response = new ForgotPwdResponse();
//                    response = forgotPwdManager.delete(usrResponse.getToken());
//                    //			log.info("Invalid Token has been removed " + response);
//                    usrResponse = forgotPwdManager.addDetails(to);
//
//                }
//
//            } catch (Exception ex) {
//                usrResponse = forgotPwdManager.addDetails(to);
//            }
//            //	log.info("Mail service token: " + usrResponse.getToken());
//
//            String link = qaddoo + usrResponse.getToken();
//            Properties props = new Properties();
//
//            props.put(mail_smtp_auth, "false");
//            props.put(mail_smtp_starttls_enable, "true");
//            props.put(mail_smtp_host, mail_smtp_host_ip);
//            props.put(mail_smtp_port, mail_smtp_port_no);
//            props.put(mail_smtp_ssl_trust, mail_smtp_host_ip);
//            props.put(mail_debug, "true");
//
//            Session session = Session.getInstance(props);
//            // try{
//            MimeMessage message = new MimeMessage(session);
//
//            List<String> recipients = Arrays.asList(request.getParameter("email"));
//            message.setFrom(new InternetAddress("system@qaddoo.com"));
//            message.setReplyTo(new Address[]{new InternetAddress("noreply@qaddoo.com")});
//            for (String recipient : recipients) {
//                message.addRecipient(Message.RecipientType.BCC, new InternetAddress(recipient));
//            }
//            String htmlBody = "<h4>Please open the link in your phone browser to reset your password </h4>"
//                    + "<a href=\'" + link + "\'>click the link to reset your password</a>";
//            message.setSubject(subject);
//            message.setContent(htmlBody, "text/html");
//            // message.setText(htmlBody,"UTF-8","html");
//            Transport.send(message);
//            //	log.info("Mail sent to reset password ");
//            // return "Mail sent to reset password, check your Junk/spam folder";
//            dtResponse.setDeviceId(usrResponse.getToken());
//            return ServiceUtils.setResponse(dtResponse, true, "Mail sent to reset password, check ur Junk/spam folder");
//
//        } catch (ValidationException e) {
//            return ServiceUtils.setResponse(dtResponse, false, e.getMessage());
//        } catch (Exception e) {
//            // return "Failed to send reset email:"+e.getMessage();
//            return ServiceUtils.setResponse(dtResponse, false, "Failed to send reset email");
//        }
//
//    }
//
//    @RequestMapping(value = "/token", method = RequestMethod.PUT)
//    public @ResponseBody
//    ForgotPwdResponse updateUser(@RequestBody ForgotPwdRequest request) {
//        ForgotPwdResponse response = new ForgotPwdResponse();
//        try {
//            response = forgotPwdManager.addDetails(request.getUser());
//            // userManager.updatePassword(request)
//            //(ServiceUtils.createAuthObj(authToken),
//            // request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Generate token to reset pwd", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/deltoken", method = RequestMethod.DELETE)
//    public @ResponseBody
//    ForgotPwdResponse deleteUser(@RequestBody ForgotPwdRequest request) {
//        ForgotPwdResponse response = new ForgotPwdResponse();
//        try {
//            response = forgotPwdManager.delete(request.getToken());
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Delete pwd reset token ", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/gettoken", method = RequestMethod.POST)
//    public @ResponseBody
//    ForgotPwdResponse validate(@RequestBody ForgotPwdRequest request) {
//        ForgotPwdResponse response = new ForgotPwdResponse();
//        try {
//            if (!Strings.isNullOrEmpty(request.getUser())) {
//                response = forgotPwdManager.getDetailsByUser(request.getUser());
//            }
//            if (!Strings.isNullOrEmpty(request.getToken())) {
//                response = forgotPwdManager.getDetailsByToken(request.getToken());
//            }
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Token fetch failed ", e);
//        }
//        return response;
//    }
//}
