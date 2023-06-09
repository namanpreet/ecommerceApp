package com.qaddoo.controller;

import com.qaddoo.pojo.entity.AddMessageRequest;
import com.qaddoo.pojo.entity.AddMessageResponse;
import com.qaddoo.pojo.entity.ListMessageResponse;
import com.qaddoo.service.manager.MessageMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/message")
public class MessageController {

    private final MessageMgmt messageManager;

    public MessageController(MessageMgmt messageManager) {
        this.messageManager = messageManager;
    }

    @PostMapping(value = "/add")
    public AddMessageResponse createMessage(@RequestHeader("authToken") String authToken,
                                            @RequestBody AddMessageRequest request) {

        AddMessageResponse response = new AddMessageResponse();

        try {
            response = messageManager.createMessage(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Create Post", e);
        }

        return response;
    }

    // available for exploring without login
    // list comments on a post
    @GetMapping(value = "/list/{forumId}")
    public ListMessageResponse listMessage(@RequestHeader(value = "authToken", required = false) String authToken,
                                           @PathVariable("forumId") long forumId) {
        ListMessageResponse response = new ListMessageResponse();
        try {
            response = messageManager.listMessage(ServiceUtils.createAuthObj(authToken), forumId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Message", e);
        }
        return response;
    }

    // list message
    @GetMapping(value = "/search/{forumId}/{searchString}")
    public ListMessageResponse searchMessage(@RequestHeader("authToken") String authToken,
                                             @PathVariable("forumId") long forumId,
                                             @PathVariable("searchString") String searchString) {

        ListMessageResponse response = new ListMessageResponse();

        try {
            response = messageManager.searchMessage(ServiceUtils.createAuthObj(authToken), forumId, searchString.trim());
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Search Message", e);
        }

        return response;
    }

}
