package com.qaddoo.controller;

import com.qaddoo.pojo.entity.*;
import com.qaddoo.service.manager.WallMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/wall")
public class WallController {

    private final WallMgmt wallMgmt;

    public WallController(WallMgmt wallMgmt) {
        this.wallMgmt = wallMgmt;
    }

    @PostMapping(value = "/add")
    public AddWallPostResponse createWall(@RequestHeader("authToken") String authToken,
                                          @RequestBody AddWallPostRequest request) {

        AddWallPostResponse response = new AddWallPostResponse();

        try {
            response = wallMgmt.createWallPost(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Create Wall Post", e);
        }

        return response;
    }

    @PutMapping(value = "/update")
    public UpdateWallPostResponse updateWall(@RequestHeader("authToken") String authToken,
                                             @RequestBody UpdateWallPostRequest request) {

        UpdateWallPostResponse response = new UpdateWallPostResponse();

        try {
            response = wallMgmt.updateWallPost(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Update Wall Post", e);
        }

        return response;
    }

    // available for exploring without login
    @GetMapping(value = "/list/{tanId}")
    public ListWallPostResponse listWalls(@RequestHeader(value = "authToken", required = false) String authToken,
                                          @PathVariable("tanId") long tanId) {

        ListWallPostResponse response = new ListWallPostResponse();

        try {
            response = wallMgmt.listWall(ServiceUtils.createAuthObj(authToken), tanId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List tan walls", e);
        }

        return response;
    }

    @DeleteMapping(value = "/delete/{wallId}")
    public DeleteWallPostResponse deleteWall(@RequestHeader("authToken") String authToken,
                                      @PathVariable("wallId") long wallId) {

        DeleteWallPostResponse response = new DeleteWallPostResponse();

        try {
            response = wallMgmt.deleteWall(ServiceUtils.createAuthObj(authToken), wallId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Delete Wall Post", e);
        }

        return response;
    }

    @PostMapping(value = "/reaction")
    public SuccessResponse wallReaction(@RequestHeader("authToken") String authToken,
                                           @RequestBody WallReactionRequest request) {

        SuccessResponse response = new SuccessResponse();

        try {
            response = wallMgmt.wallReaction(ServiceUtils.createAuthObj(authToken), request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add wall reaction", e);
        }

        return response;
    }
    // available for exploring without login
    @GetMapping(value = "/get/{wallId}")
    public GetWallResponse getWallDetails(@RequestHeader(value = "authToken", required = false) String authToken,
                                          @PathVariable("wallId") long wallId) {
        GetWallResponse response = new GetWallResponse();
        try {
            response = wallMgmt.getWallDetails(ServiceUtils.createAuthObj(authToken), wallId);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Wall Details", e);
        }
        return response;
    }

}
