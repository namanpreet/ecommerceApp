//package com.qaddoo.controller;
//
//import com.qaddoo.service.manager.EventMgmt;
//import com.qaddoo.service.util.ServiceUtils;
//import org.springframework.stereotype.Controller;
//import org.springframework.web.bind.annotation.*;
//import com.qaddoo.pojo.entity.*;
//
//@Controller
//@RequestMapping(value = "/event")
//public class EventController {
//
//    private final EventMgmt eventManager;
//
//    public EventController(EventMgmt eventManager) {
//        this.eventManager = eventManager;
//    }
//
//    // Create event
//    @RequestMapping(value = "/create", method = RequestMethod.PUT)
//    public @ResponseBody
//    AddEventResponse createEvent(@RequestHeader("authToken") String authToken,
//                                 @RequestBody AddEventRequest request) {
//
//        AddEventResponse response = new AddEventResponse();
//        try {
//            response = eventManager.createEvent(ServiceUtils.createAuthObj(authToken), request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Create Event", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/list", method = RequestMethod.PUT)
//    public @ResponseBody
//    ListEventResponse getEventList(@RequestHeader("authToken") String authToken,
//                                   @RequestBody ListEventRequest request) {
//
//        ListEventResponse response = new ListEventResponse();
//        try {
//            response = eventManager.getEventList(ServiceUtils.createAuthObj(authToken), request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Tan Event List", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/subscribe", method = RequestMethod.POST)
//    public @ResponseBody
//    SubscribeEventResponse subscribeEvent(@RequestHeader("authToken") String authToken,
//                                          @RequestBody SubscribeEventRequest request) {
//        SubscribeEventResponse response = new SubscribeEventResponse();
//        try {
//            response = eventManager.subscribeEvent(ServiceUtils.createAuthObj(authToken), request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/unsubscribe", method = RequestMethod.POST)
//    public @ResponseBody
//    SubscribeEventResponse unsubscribeEvent(@RequestHeader("authToken") String authToken,
//                                            @RequestBody SubscribeEventRequest request) {
//        SubscribeEventResponse response = new SubscribeEventResponse();
//        try {
//            response = eventManager.unsubscribeEvent(ServiceUtils.createAuthObj(authToken), request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "", e);
//        }
//        return response;
//    }
//
//    @RequestMapping(value = "/delete", method = RequestMethod.POST)
//    public @ResponseBody
//    DeleteEventResponse deleteEvent(@RequestHeader("authToken") String authToken,
//                                    @RequestBody DeleteEventRequest request) {
//        DeleteEventResponse response = new DeleteEventResponse();
//        try {
//            response = eventManager.deleteEvent(ServiceUtils.createAuthObj(authToken), request);
//        } catch (Exception e) {
//            return ServiceUtils.setResponse(response, false, "Delete Tan", e);
//        }
//        return response;
//    }
//
//}