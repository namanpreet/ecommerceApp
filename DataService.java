package com.qaddoo.qaddooanalytics.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.qaddoo.qaddooanalytics.model.*;
import com.qaddoo.qaddooanalytics.pojo.ReferralStoreAttrs;
import com.qaddoo.qaddooanalytics.pojo.TanAttrs;
import com.qaddoo.qaddooanalytics.pojo.TicketAttrs;
import com.qaddoo.qaddooanalytics.pojo.UserAttrs;
import com.qaddoo.qaddooanalytics.pojo.ondc.Item;
import com.qaddoo.qaddooanalytics.pojo.ondc.OndcOrderDetailsAttrs;
import com.qaddoo.qaddooanalytics.pojo.ondc.Provider;
import com.qaddoo.qaddooanalytics.repository.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

@Service
public class DataService {

    private final UserDAO userDAO;
    private final OrderDAO orderDAO;
    private final TanDAO tanDAO;
    private final HandleDAO handleDAO;
    private final ItemDAO itemDAO;
    private final CategoryDAO categoryDAO;
    private final ComplaintDAO complaintDAO;

    @Value("${qaddoo.url}")
    private String qaddooBackendUrl;


    public DataService(UserDAO userDAO, OrderDAO orderDAO, TanDAO tanDAO, HandleDAO handleDAO, ItemDAO itemDAO, CategoryDAO categoryDAO,
                       ComplaintDAO complaintDAO) {
        this.userDAO = userDAO;
        this.orderDAO = orderDAO;
        this.tanDAO = tanDAO;
        this.handleDAO = handleDAO;
        this.itemDAO = itemDAO;
        this.categoryDAO=categoryDAO;
        this.complaintDAO = complaintDAO;
    }

    // No. of total users
    public long userCount(Date startDate, Date endDate){
        return userDAO.userCount(startDate,endDate);
    }

    // No. of total users excluding inactive khatas added by store owners
    public long registeredUserCount(Date startDate, Date endDate) {
        return userDAO.registeredUserCount(startDate, endDate);
    }

    // No. of active users who have updated profile
    public long profileUserCount(Date startDate, Date endDate, String language) {
        return userDAO.profileUserCount(startDate, endDate, language);
    }

    // No. of users exploring the pp without logging in
    public long exploreUserCount(Date startDate, Date endDate, String language) {
        return userDAO.exploreUserCount(startDate, endDate, language);
    }

    // No. of users logging in using store referral code
    public long referredUserCount(Date startDate, Date endDate) {
        return userDAO.referredUserCount(startDate, endDate);
    }

    // No. of inactive khatas added by store owners
    public long inactiveUserCount(Date startDate, Date endDate) {
        return userDAO.inactiveUserCount(startDate, endDate);
    }

    public HashMap<String,Long> allUserCount (Date startDate, Date endDate, String language) {
        HashMap<String, Long> allusercount = new HashMap<>();

        allusercount.put("InactiveUserCount",userDAO.inactiveUserCount(startDate, endDate));
        allusercount.put("ProfileUserCount",userDAO.profileUserCount(startDate, endDate, language));
        allusercount.put("ExploreUserCount",userDAO.exploreUserCount(startDate,endDate,language));
        allusercount.put("ReferredUserCount",userDAO.referredUserCount(startDate, endDate));
        allusercount.put("RegisteredUserCount",userDAO.registeredUserCount(startDate, endDate));
        allusercount.put("GroupCount",tanDAO.groupCount(startDate, endDate));
        allusercount.put("QoinUserCount",userDAO.qoinUserCount(startDate, endDate));

        return allusercount;

    }

    // Details of users
    public List<UserAttrs> userDetails(Date startDate, Date endDate, boolean sort, Integer page) {
        return userDAO.userDetails(startDate, endDate, sort, page);
    }

    // Details of explore users
    public List<ExploreUserDTO> exploreUserDetails(Date startDate, Date endDate) {
        return userDAO.exploreUserDetails(startDate, endDate);
    }

    // No. of interested sellers
    public long sellerCount(Date startDate, Date endDate) {
        return userDAO.sellerCount(startDate, endDate);
    }

    // No. of interested buyers
    public long buyerCount(Date startDate, Date endDate) {
        return userDAO.buyerCount(startDate, endDate);
    }

    public OrderDTO getOrderByUid(String uid) {
        return orderDAO.getOrderByUid(uid);
    }

    public TanDTO getStoreById(long storeId) {
        return tanDAO.getStoreById(storeId);
    }

    // No. of orders
    public long orderCount(Date startDate, Date endDate, String status) {
        return orderDAO.orderCount(startDate, endDate, status);
    }

    // Details of orders
    public List<OrderDTO> orderDetails(Date startDate, Date endDate, String status, boolean sort) {
        return orderDAO.orderDetails(startDate, endDate, status, sort);
    }

    public List<OrderAttrs> orderDetailsRequired(Date startDate, Date endDate, String status, boolean sort, Integer page) {
        return orderDAO.orderDetailsRequired(startDate, endDate, status, sort, page);
    }

    public Map<String, BigDecimal> orderLabels(Date startDate, Date endDate) {
        return orderDAO.orderLabels(startDate, endDate);
    }

    // Total value of all orders
    public BigDecimal orderValue(Date startDate, Date endDate) {
        List<OrderDTO> orderDTOList = orderDAO.valueOrders(startDate, endDate);

        BigDecimal value = BigDecimal.ZERO;

        if (Objects.nonNull(orderDTOList) && !orderDTOList.isEmpty()) {
            for (OrderDTO orderDTO : orderDTOList) {
                value = value.add(orderDTO.getAmount());
            }
        }

        return value;
    }

    // No. of groups
    public long groupCount(Date startDate, Date endDate) {
        return tanDAO.groupCount(startDate, endDate);
    }

    // Details of groups
    public List<TanDTO> groupDetails(Date startDate, Date endDate, boolean sort) {
        return tanDAO.groupDetails(startDate, endDate, sort);
    }

    // No. of stores
    public long storeCount(Date startDate, Date endDate) {
        return tanDAO.storeCount(startDate, endDate);
    }

    //No. of stores using qoins
    public long qoinStoreCount(Date startDate, Date endDate) {
        return tanDAO.qoinStoreCount(startDate, endDate);
    }

    // Details of stores
    public List<TanAttrs> storeDetails(Date startDate, Date endDate, boolean sort, Integer page) {
        return tanDAO.storeDetails(startDate, endDate, sort, page);
    }

    // Referral stores
    public List<ReferralStoreAttrs> referralStores(Date startDate, Date endDate) {
        return tanDAO.getReferralStores(startDate, endDate);
    }

    // No. of unique countries
    public List<String> uniqueCountryCount() {
        return userDAO.uniqueCountryCount();
    }

    // No. of unique cities
    public long uniqueCityCount() {
        return userDAO.uniqueCityCount();
    }

    // No. of unique cities in India
    public long uniqueCityCountIndia() {
        return userDAO.uniqueCityCountIndia();
    }

    // Total balance of all customers in khata
    public BigDecimal totalBalance() {
        return handleDAO.totalBalance();
    }

    // No. of unique products ordered
    public long itemCount() {
        return itemDAO.itemCount();
    }

    public HashMap<String, Long> allSellerCount(Date startDate, Date endDate, String status){
        HashMap<String , Long> allsellercount = new HashMap<String, Long>();

        allsellercount.put("StoreCount",tanDAO.storeCount(startDate, endDate));
        allsellercount.put("OrderCount",orderDAO.orderCount(startDate,endDate,status));
        allsellercount.put("SellerCount",userDAO.sellerCount(startDate, endDate));
        allsellercount.put("BuyerCount",userDAO.buyerCount(startDate, endDate));
        allsellercount.put("ReferredUserCount",userDAO.referredUserCount(startDate,endDate));
        allsellercount.put("ShopQoinsPlan", tanDAO.qoinStoreCount(startDate, endDate));

        return allsellercount;
    }
    public String getCategoryName(Long categoryId) {
        return categoryDAO.getCategoryName(categoryId);
    }

    public List<TicketAttrs> listRaisedTickets(int page, String status) {
        List<ComplaintDTO> complaintDTOList = complaintDAO.getRaisedTickets(page, status);
        List<TicketAttrs> ticketAttrsList = new ArrayList<>();
        for (ComplaintDTO complaintDTO : complaintDTOList) {
            TicketAttrs ticketAttr = new TicketAttrs();
            ticketAttr.setTicketId(complaintDTO.getId());
            ticketAttr.setStatus(complaintDTO.getComplaintStatus());
            ticketAttr.setComplaintTime(complaintDTO.getComplaintTime());
            ticketAttr.setConversation(complaintDTO.getConversation());
            ticketAttrsList.add(ticketAttr);
        }
        return ticketAttrsList;
    }

    /**
     * Return order items, fulfillment info so support team and understand and resolve issue fast *
     * @param id
     * @return
     */
    public TicketAttrs getTicketDetails(long id) {
        ComplaintDTO complaintDTO = complaintDAO.getTicketDetails(id);
        TicketAttrs ticketAttrs = new TicketAttrs();
        ticketAttrs.setTicketId(complaintDTO.getId());
        ticketAttrs.setConversation(complaintDTO.getConversation());
        ticketAttrs.setStatus(complaintDTO.getComplaintStatus());
        ticketAttrs.setComplaintTime(complaintDTO.getComplaintTime());

        ObjectMapper mapper = new ObjectMapper();

        if (complaintDTO.isOndcOrder()) {
            OndcOrderDTO ondcOrderDTO = orderDAO.getOndcOrderById(complaintDTO.getOrderId());
            OndcOrderDetailsAttrs ondcOrderAttrs = new OndcOrderDetailsAttrs();
            ondcOrderAttrs.setOrderId(ondcOrderDTO.getOrderId());
            ondcOrderAttrs.setQuote(ondcOrderDTO.getQuote());
            ondcOrderAttrs.setState(ondcOrderDTO.getState());

            Provider provider = mapper.convertValue(ondcOrderDTO.getProvider(), new TypeReference<Provider>() {});
            if (Objects.nonNull(ondcOrderDTO.getUpdatedItems())) {
                List<Item> items = (List<Item>) (Object) ondcOrderDTO.getItems();
                ondcOrderAttrs.setItems(items);
            } else {
                ondcOrderAttrs.setItems(mapper.convertValue(ondcOrderDTO.getItems(), new TypeReference<List<Item>>() {}));
            }

            ticketAttrs.setOrder(ondcOrderAttrs);
        } else {

        }

        return ticketAttrs;
    }

    public ResponseEntity<String> updateTicketBySupportDashboard(UpdateRaisedTicketRequest request) {

        ComplaintDTO complaintDTO = complaintDAO.getTicketDetails(request.getTicketId());
        if(Objects.isNull(complaintDTO)) {
            return ResponseEntity.badRequest().body("Invalid ticket to update");
        } else if(complaintDTO.getComplaintStatus().equals("CLOSED")) {
            return ResponseEntity.badRequest().body("Ticket is already Closed");
        }

        if (Objects.nonNull(request.getType())) {
            complaintDTO.setComplaintType(request.getType());
        }

        if (!Strings.isNullOrEmpty(request.getMessage())) {
            List<SupportMessage> messageList = complaintDTO.getConversation();
            SupportMessage teamResponse = new SupportMessage();
            teamResponse.setMessage(request.getMessage());
            teamResponse.setSender("team");
            teamResponse.setTime(new Timestamp(System.currentTimeMillis()));
            messageList.add(teamResponse);
            complaintDTO.setConversation(messageList);
        }

        //from support dashboard it can be ticket status can be updated OPEN, CLOSE
        if (!Strings.isNullOrEmpty(request.getComplaintStatus())) {
            complaintDTO.setComplaintStatus(request.getComplaintStatus());
            if(request.getComplaintStatus().equals("CLOSED")) {
                complaintDTO.setCloseTime(new Timestamp(System.currentTimeMillis()));

                //if request is cascaded close the forwarded ticket with seller as well
                if (complaintDTO.isRequestCascaded()) {

                }
            }
        }

        try {
            complaintDAO.updateTicket(complaintDTO);

            //notify the user with notification as well
            String req = "{\"ticketId\":"+ request.getTicketId() +"}";
            String notificationUrl = qaddooBackendUrl + "/fcm/support/response";
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(notificationUrl);
            uriBuilder.queryParam("password", "Milton_Fanny@2021");
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            HttpEntity<String> httpEntity = new HttpEntity<>(req, headers);
            ResponseEntity<String> result = restTemplate.exchange(uriBuilder.build().toUriString(), HttpMethod.POST, httpEntity, String.class);


        } catch (Exception e) {
            return ResponseEntity.ok().body("");
        }

        return ResponseEntity.ok().body("query responded successfully");
    }

    public ResponseEntity<String> forwardQueryToCascadedParty(ForwardTicketRequest request) {

        ComplaintDTO complaintDTO = complaintDAO.getTicketDetails(Long.parseLong(request.getTicketId()));
        if(Objects.isNull(complaintDTO))
            return ResponseEntity.badRequest().body("Invalid ticket Id");

        /*if(!complaintDTO.isRequestCascaded()) {
            complaintDTO.setCategory(request.getCategory());
            complaintDTO.setSubCategory(request.getSubCategory());
            complaintDTO.setIssueType(request.getIssueType());
        }*/

        ObjectMapper mapper = new ObjectMapper();
        String req = null;
        try {
            req = mapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String qaddooUrl = qaddooBackendUrl + "/support/ondc/forward/ticket";
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> httpEntity = new HttpEntity<>(req, headers);
        try {
            ResponseEntity<String> result = restTemplate.exchange(qaddooUrl, HttpMethod.PUT, httpEntity, String.class);
            if (result.getStatusCode().is2xxSuccessful()) {
                return ResponseEntity.ok().body("query forwarded successfully");
            } else {
                return ResponseEntity.badRequest().body(result.getBody());
            }
        } catch (Exception ignored) {

        }

        return ResponseEntity.ok().body("query forwarded successfully");
    }

    public LoginDTO getLoginDetails(String email, String password) throws Exception {
         return userDAO.getLoginDetails(email,password);

    }
}
