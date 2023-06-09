package com.qaddoo.controller;

import com.qaddoo.pojo.entity.SendWallPostMailResponse;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.stereotype.Service;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Service
public class WallPostNotification {

    private static final String mail_smtp_host_ip = "localhost";
    private static final String mail_smtp_port_no = "25";
    private static final String mail_smtp_auth = "mail.smtp.auth";
    private static final String mail_smtp_starttls_enable = "mail.smtp.starttls.enable";
    private static final String mail_smtp_host = "mail.smtp.host";
    private static final String mail_smtp_port = "mail.smtp.port";
    private static final String mail_smtp_ssl_trust = "mail.smtp.ssl.trust";
    private static final String mail_debug = "mail.debug";
    private static final String subject = "WallPost For user";
    private static final String qaddoo = "http://platform.qaddoo.com:8080/qaddoo-restful-service/openresetpage.jsp?token=";

    public boolean sendEmail(String emailId, String tanID, String wallID) {
        // ForgotPwdResponse userResponse = new ForgotPwdResponse();
        SendWallPostMailResponse Response = new SendWallPostMailResponse();
        try {
            // log.info("Mail service starts !!!");
            String to = emailId;

            // String link = qaddoo + usrResponse.getToken();
            Properties props = new Properties();

            props.put(mail_smtp_auth, "false");
            props.put(mail_smtp_starttls_enable, "true");
            props.put(mail_smtp_host, mail_smtp_host_ip);
            props.put(mail_smtp_port, mail_smtp_port_no);
            props.put(mail_smtp_ssl_trust, mail_smtp_host_ip);
            props.put(mail_debug, "true");

            Session session = Session.getInstance(props);
            // try{
            MimeMessage message = new MimeMessage(session);

            List<String> recipients = Arrays.asList(emailId);
            message.setFrom(new InternetAddress("system@qaddoo.com"));
            message.setReplyTo(new Address[]{new InternetAddress("noreply@qaddoo.com")});
            for (String recipient : recipients) {
                message.addRecipient(Message.RecipientType.BCC, new InternetAddress(recipient));
            }
            // + "&wallid=" + wallID;
            // String link = qaddoo + "tanid=" + tanID ;
            String link = qaddoo + "_tanid=" + tanID + "_wallid=" + wallID;

            // String link = qaddoo + URLEncoder.encode(tanID, "UTF-8") +
            // URLEncoder.encode(wallID, "UTF-8");

            String linkDownloadIOS = "https://itunes.apple.com/us/app/qaddoo/id1049202034?mt=8";
            String linkDownloadAndroid = "https://play.google.com/store/apps/details?id=com.qaddoo";

            String htmlBody = "<h4>Please Go to the TAN On loction to Check Wall Specially for you </h4>" + "<a href=\'"
                    + link + "\'>click the link to See wall.</a><br><br>" + "<a href=\'" + linkDownloadIOS
                    + "\'>click the link to Download from Apple App Store.</a><br><br>" + "<a href=\'"
                    + linkDownloadAndroid + "\'>click the link to Download from Google Play Store.</a>";

            // http://platform.qaddoo.com:8080/qaddoo-restful-service/openresetpage.jsp?token=tanid=4255,wallid=846

            message.setSubject(subject);
            message.setContent(htmlBody, "text/html");
            // message.setText(htmlBody,"UTF-8","html");
            Transport.send(message);
            // log.info("Mail sent to reset password ");
            // return "Mail sent to reset password, check your Junk/spam folder";
            // dtResponse.setDeviceId(usrResponse.getToken());
            return ServiceUtils.setResponse(Response, true,
                    "Mail sent to inform about WallPost, check your Junk/spam folder") != null;

        } catch (Exception e) {
            // return "Failed to send reset email:"+e.getMessage();
            return ServiceUtils.setResponse(Response, false, "Failed to send WallPost email") != null;
        }

    }
}
