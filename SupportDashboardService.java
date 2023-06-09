package com.qaddoo.qaddooanalytics.service;

import com.qaddoo.qaddooanalytics.model.SupportAttrs;
import com.qaddoo.qaddooanalytics.repository.SupportDashboardDAO;
import org.springframework.stereotype.Service;

@Service
public class SupportDashboardService {

    private final SupportDashboardDAO supportDashboardDAO;

    public SupportDashboardService(SupportDashboardDAO supportDashboardDAO) {
        this.supportDashboardDAO = supportDashboardDAO;
    }


    public SupportAttrs getSupportDetails() {
        return supportDashboardDAO.getSupportDetails();
    }
}
