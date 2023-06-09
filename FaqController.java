package com.qaddoo.controller;

import com.qaddoo.pojo.entity.AddFaqRequest;
import com.qaddoo.pojo.entity.FaqResponse;
import com.qaddoo.pojo.entity.ListFaqRequest;
import com.qaddoo.pojo.entity.ListFaqResponse;
import com.qaddoo.service.manager.FaqMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/faq")
public class FaqController {

    private final FaqMgmt faqMgmt;

    public FaqController(FaqMgmt faqMgmt) {
        this.faqMgmt = faqMgmt;
    }

    @PostMapping(value = "/add")
    public FaqResponse addFaq(@RequestBody AddFaqRequest request) {

        FaqResponse response = new FaqResponse();

        try {
            response = faqMgmt.addFaq(request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add FAQ", e);
        }

        return response;
    }

    // available for exploring without login
    @PutMapping(value = "/list")
    public ListFaqResponse listFaqs(@RequestHeader(value = "authToken", required = false) String authToken,
                                    @RequestParam(value = "language", defaultValue = "english") String language,
                                    @RequestBody ListFaqRequest request) {

        ListFaqResponse response = new ListFaqResponse();

        try {
            response = faqMgmt.listFaqs(ServiceUtils.createAuthObj(authToken), request, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List FAQs", e);
        }

        return response;
    }
}
