package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.ForumMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/forum")
public class ForumController {

    private final ForumMgmt forumManager;
    private static final Logger logger = LoggerFactory.getLogger(ForumController.class);

    public ForumController(ForumMgmt forumManager) {
        this.forumManager = forumManager;
    }


    // add Forum
    @PostMapping(value = "/add")
    public AddForumResponse createForum(@RequestHeader("authToken") String authToken,
                                        @RequestBody AddForumRequest request) {
        logger.debug("ForumController.createforum----Enter---" + request);
        AddForumResponse response = new AddForumResponse();
        try {
            response = forumManager.createForum(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            logger.warn("ForumController.createForum----Exception in creating a Forum----" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Add Forum", e);
        }
        logger.debug("ForumController.createForum ----Exit---");
        return response;
    }

    // update Forum
    @PutMapping(value = "/update")
    public UpdateForumResponse updateForum(@RequestHeader("authToken") String authToken,
                                           @RequestBody UpdateForumRequest request) {
        logger.debug("ForumController.updateForum----Enter----" + request);
        UpdateForumResponse response = new UpdateForumResponse();
        try {
            response = forumManager.updateForum(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            logger.warn("ForumController.updateForum---Exception in updating forum." + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Update Forum", e);
        }
        logger.debug("ForumController.updateForum----Exit--");
        return response;
    }

    // available for exploring without login
    @GetMapping(value = "/get/{forumId}")
    public GetForumResponse getForum(@RequestHeader(value = "authToken", required = false) String authToken,
                                     @PathVariable("forumId") long forumId) {
        GetForumResponse response = new GetForumResponse();
        try {
            response = forumManager.getForum(ServiceUtils.createAuthObj(authToken), forumId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Forum Details", e);
        }
        return response;
    }

    // 12/10/2021 (4.0.10) and previous releases
    @Deprecated
    @GetMapping(value = "/list/{tanid}")
    public ListForumResponse listForum(@RequestHeader("authToken") String authToken,
                                       @PathVariable("tanid") long tanId) {
        logger.debug("ForumController.listForum----Enter--tanID:" + tanId);
        ListForumResponse response = new ListForumResponse();
        try {
            response = forumManager.listForum(ServiceUtils.createAuthObj(authToken), tanId);
        } catch (Exception e) {
            logger.warn("ForumController.listForums-----Exception in fetching list of Forums---" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "List Forum", e);
        }
        logger.debug("ForumController.listForums----Exit----");
        return response;
    }

    // available for exploring without login
    // 2/11/2021 release (4.0.11)
    @GetMapping(value = "/list/v1/{tanid}", params = {"page"})
    public ListForumResponse listForumV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                         @RequestParam("page") int page, @PathVariable("tanid") long tanId) {
        logger.debug("ForumController.listForum----Enter--tanID:" + tanId);
        ListForumResponse response = new ListForumResponse();
        try {
            response = forumManager.listForumV1(ServiceUtils.createAuthObj(authToken), tanId, page);
        } catch (Exception e) {
            logger.warn("ForumController.listForums-----Exception in fetching list of Forums---" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "List Forum", e);
        }
        logger.debug("ForumController.listForums----Exit----");
        return response;
    }

    @GetMapping(value = "/list/{tanid}/{searchstring}")
    public ListForumResponse searchForum(@RequestHeader("authToken") String authToken,
                                         @PathVariable("tanid") long tanId, @PathVariable("searchstring") String searchString) {
        logger.debug("ForumController.searchForum----Enter--- tanId:" + tanId + "--searchString :" + searchString);
        ListForumResponse response = new ListForumResponse();
        try {
            response = forumManager.searchForum(ServiceUtils.createAuthObj(authToken), tanId, searchString.trim());
        } catch (Exception e) {
            logger.warn("ForumController.searchForum-----Exception in searching Forum---" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Search Forum", e);
        }
        logger.debug("ForumController.searchForum------Exit----");
        return response;
    }

    // delete forum
    @DeleteMapping(value = "/delete/{forumId}")
    public DeleteForumResponse deleteForum(@RequestHeader("authToken") String authToken,
                                           @PathVariable("forumId") long forumId) {
        logger.debug("ForumController.deleteForum-----Enter-----ForumID:" + forumId);
        DeleteForumResponse response = new DeleteForumResponse();
        try {
            response = forumManager.deleteForum(ServiceUtils.createAuthObj(authToken), forumId);
        } catch (Exception e) {
            logger.warn("ForumController.deleteForum----Exception in deleting Forum--------" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Delete Forum", e);
        }
        logger.debug("ForumController.deleteForum-----Exit---");
        return response;
    }


    // 30/12/2021 (4.0.14) and previous releases
    @Deprecated
    @PutMapping(value = "/reaction")
    public LikeForumResponse likeForum(@RequestHeader("authToken") String authToken,
                                       @RequestBody LikeForumRequest request) {
        logger.debug("ForumController.likeForum----Enter-----" + request);
        LikeForumResponse response = new LikeForumResponse();
        try {
            response = forumManager.likeForum(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            logger.warn("ForumController.likeForum-----Exception in liking Forum----" + e.getMessage());
            return ServiceUtils.setResponse(response, false, "Like Forum", e);
        }
        logger.debug("ForumController.likeForum------Exit----");
        return response;
    }

    @PutMapping(value = "/reaction/v1")
    public SuccessResponse forumReaction(@RequestHeader("authToken") String authToken,
                                         @RequestBody ForumReactionRequest request) {
        SuccessResponse response = new SuccessResponse();

        try {
            response = forumManager.forumReaction(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add post reaction", e);
        }

        return response;
    }

    // this endpoint was used to add userId to every existing reaction in the database
    // was used only once, no longer used and required
    @PutMapping(value = "/reaction/addUserId")
    public SuccessResponse addUserIdToReactions(@RequestParam("password") String password) {
        SuccessResponse response = new SuccessResponse();
        response.setMessage("No operation performed");

//        try {
//            response = forumManager.addUserIdToReactions(password);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Add userId to reactions", e);
//        }

        return response;
    }

}
