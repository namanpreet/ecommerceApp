package com.qaddoo.controller;

import com.qaddoo.pojo.entity.AddCategoryRequest;
import com.qaddoo.pojo.entity.AddCategoryResponse;
import com.qaddoo.pojo.entity.ListCategoryResponse;
import com.qaddoo.service.manager.CategoryMgmt;
import com.qaddoo.service.util.ServiceUtils;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/category")
public class CategoryController {

    private final CategoryMgmt categoryMgmt;

    public CategoryController(CategoryMgmt categoryMgmt) {
        this.categoryMgmt = categoryMgmt;
    }

    @PostMapping(value = "/add")
    public AddCategoryResponse addCategory(@RequestBody AddCategoryRequest request) {

        AddCategoryResponse response = new AddCategoryResponse();

        try {
            response = categoryMgmt.addCategory(request);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "Add Category", e);
        }

        return response;
    }

    // 14/12/2021 and previous releases (4.0.14)
    @Deprecated
    @GetMapping(value = "/list")
    public ListCategoryResponse listCategories(@RequestHeader("authToken") String authToken) {

        ListCategoryResponse response = new ListCategoryResponse();

        try {
            response = categoryMgmt.listCategories(ServiceUtils.createAuthObj(authToken));
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Categories", e);
        }

        return response;
    }

    // available for exploring without login
    // 30/12/2021 release (4.0.15)
    @GetMapping(value = "/list/v1")
    public ListCategoryResponse listCategoriesV1(@RequestHeader(value = "authToken", required = false) String authToken,
                                                 @RequestParam("requireItemCount") boolean requireItemCount,
                                                 @RequestParam(value = "language", defaultValue = "english") String language) {

        ListCategoryResponse response = new ListCategoryResponse();

        try {
            response = categoryMgmt.listCategoriesV1(ServiceUtils.createAuthObj(authToken), requireItemCount, language);
        } catch (Exception e) {
            return ServiceUtils.setResponse(response, false, "List Categories", e);
        }

        return response;
    }
}
