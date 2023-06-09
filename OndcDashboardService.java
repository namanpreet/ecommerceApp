package com.qaddoo.qaddooanalytics.service;

import com.qaddoo.qaddooanalytics.model.*;
import com.qaddoo.qaddooanalytics.repository.*;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class OndcDashboardService {

    private final OndcDashboardDAO ondcDashboardDao;

    public OndcDashboardService(OndcDashboardDAO ondcDashboardDao) {
        this.ondcDashboardDao = ondcDashboardDao;
    }


    public List<OndcOrderAttrs> getOndcOrders() {
        return ondcDashboardDao.getOndcOrders();
    }

    public List<PaymentOndcOrderAttrs> getPaymentOndcOrders() {
        return ondcDashboardDao.getPaymentOndcOrders();
    }

    public List<ComplaintTicketsAttrs> getComplaintTickets() {
//        List<ComplaintDTO> complaintDTOList = ondcDashboardDao.getComplaintTickets();
        List<ComplaintTicketsAttrs> complaintTicketsAttrsList = ondcDashboardDao.getComplaintTickets();
        /*complaintDTOList.forEach(complaintDTO -> {
            ComplaintTicketsAttrs ticketsAttrs = new ComplaintTicketsAttrs();

            ticketsAttrs.setTicketId(String.valueOf(complaintDTO.getId()));
            ticketsAttrs.setNetworkOrderId(String.valueOf(complaintDTO.getOrderId()));
            ticketsAttrs.setOrderId(String.valueOf(complaintDTO.getOrderId()));

            ticketsAttrs.setBuyerNP("QADDOO");


            complaintTicketsAttrsList.add(ticketsAttrs);
        });*/

        return complaintTicketsAttrsList;
    }

    public List<NotificationDTO> getDashboardNotifications() {

        List<NotificationDTO> notificationDTOList = ondcDashboardDao.getDashboardNotifications();
        return  notificationDTOList;
    }
}
