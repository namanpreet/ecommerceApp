//package com.qaddoo.persistence.dao.impl;
//
//import com.qaddoo.persistence.CommonDAOImpl;
//import com.qaddoo.persistence.dao.EventDAO;
//import com.qaddoo.persistence.dto.EventDTO;
//import com.qaddoo.persistence.dto.EventSubscriptionDTO;
//import org.springframework.stereotype.Repository;
//
//import java.util.List;
//
//@Repository("eventDAO")
//public class EventDAOImpl extends CommonDAOImpl implements EventDAO {
//
//    @Override
//    public Long addEvent(EventDTO eventDTO) {
//        return (Long) this.saveObject(eventDTO);
//    }
//
//    @Override
//    public List<EventDTO> getEventListByTan(long tanId) {
//
//        String namedQuery = "event.findEventByTan";
//        return getObjectList(namedQuery, EventDTO.class, "tanid", tanId);
//    }
//
//    @Override
//    public void subscribeEvent(EventSubscriptionDTO subscription) {
//
//        this.saveObject(subscription);
//
//    }
//
//    @Override
//    public Object unsubscribeEvent(long userId, long eventId) {
//
//        String namedQuery = "subscribe.false";
//        return executeUpdateNamedQuery(namedQuery, new Param("userId", userId), new Param("eventId", eventId));
//
//    }
//
//    @Override
//    public boolean getSubscriptionStatus(String eventId, long userId) {
//
//        return false;
//    }
//
//    @Override
//    public EventSubscriptionDTO getEventInfo(long userId, long eventId) {
//
//        String namedQuery = "subscribe.getRecord";
//        return getSingleObjectFromNamedQuery(namedQuery, EventSubscriptionDTO.class, new Param("userId", userId),
//                new Param("eventId", eventId));
//
//    }
//
//    @Override
//    public List<EventDTO> listAllEventsByDistance(double latitude, double longitude, double d) {
//
//        String namedQuery = "subscribe.getAllActiveEvents";
//        return getObjectList(namedQuery, EventDTO.class, "lat", latitude, "lon", longitude, "d", d);
//    }
//
//    @Override
//    public Object subscribeEventAgain(long userId, long eventId) {
//
//        String namedQuery = "subscribe.true";
//        return executeUpdateNamedQuery(namedQuery, new Param("userId", userId), new Param("eventId", eventId));
//
//    }
//
//    @Override
//    public void deleteEvent(long eventId) {
//        String namedQuery = "event.delete";
//        executeUpdateNamedQuery(namedQuery, new Param("eventId", eventId));
//
//    }
//
//    @Override
//    public void deleteEventSubscribers(long eventId) {
//        String namedQuery = "subscribe.deleteSubscribers";
//        executeUpdateNamedQuery(namedQuery, new Param("eventId", eventId));
//
//    }
//
//    @Override
//    public void updateEventStatus(int eventStatus, long eventId) {
//        String namedQuery = "event.updateEventStatus";
//        executeUpdateNamedQuery(namedQuery, new Param("eventStatus", eventStatus), new Param("eventId", eventId));
//    }
//
//    @Override
//    public EventDTO getEvent(long eventId) {
//        String namedQuery = "event.findById";
//        return getSingleObjectFromNamedQuery(namedQuery, EventDTO.class, "id", eventId);
//
//    }
//
//    @Override
//    public List<Object[]> getListOfActiveEvents(Double latitude, Double longitude, Double d) {
//        String strQuery = "SELECT e.id ,e.tanId,e.layout,e.startTime,e.pattern,e.eventtitle,t.name,e.createdby,e.eventstatus,h.userid,t.secure FROM event e\n" +
//                "left outer join tans t on e.tanid=t.id\n" +
//                "left outer join handles h on e.createdby=h.name\n" +
//                " WHERE t.id IN (SELECT id from (SELECT *,( 3959 * acos (  cos ( radians(?) ) * cos( radians( cast(latitude as float) ) ) * cos( radians( cast(longitude as float) ) - radians(?) ) +  sin( radians(?) ) * sin( radians( cast(latitude as float) ) ) ) ) AS distance FROM tans) as a where distance < ? ORDER BY distance ) and e.eventstatus=1 and e.tanid=t.id  order by starttime asc LIMIT 500";
//        return getValueFromNativeQuery(strQuery, latitude, longitude, latitude, d);
//    }
//
//    @Override
//    public List<Object[]> getActiveEvents() {
//        String strQuery = "select e.id, e.eventtitle ,e.startTime,e.timezone FROM event e where  e.eventstatus=1  order by e.startTime desc";
//        return getValueFromNativeQuery(strQuery);
//    }
//}