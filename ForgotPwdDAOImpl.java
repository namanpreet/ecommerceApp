//package com.qaddoo.persistence.dao.impl;
//
//import com.qaddoo.persistence.CommonDAOImpl;
//import com.qaddoo.persistence.dao.ForgotPwdDAO;
//import com.qaddoo.persistence.dto.ForgotPwdDTO;
//import org.springframework.stereotype.Repository;
//
//import java.sql.Timestamp;
//import java.util.List;
//
//@Repository("forgotPwdDAO")
//public class ForgotPwdDAOImpl extends CommonDAOImpl implements ForgotPwdDAO {
//
//    @Override
//    public ForgotPwdDTO getUserByToken(String token) {
//        String namedQuery = "pwd.getUserByToken";
//        return getSingleObjectFromNamedQuery(namedQuery, ForgotPwdDTO.class, "token", token);
//    }
//
//    @Override
//    public ForgotPwdDTO getUserByEmail(String email) {
//        String namedQuery = "pwd.getUserByEmail";
//        return getSingleObjectFromNamedQuery(namedQuery, ForgotPwdDTO.class, "useremail", email);
//    }
//
//    @Override
//    public Long addUser(ForgotPwdDTO forgotPwdDTO) {
//        // TODO Auto-generated method stub
//        return (Long) this.saveObject(forgotPwdDTO);
//
//    }
//
//    @Override
//    public void deleteUser(ForgotPwdDTO forgotPwdDTO) {
//        // TODO Auto-generated method stub
//        this.deleteObject(forgotPwdDTO);
//
//    }
//
//    @Override
//    public void deleteExpiredTokens(Timestamp current) {
//        String namedQuery = "pwd.deleteExpiredTokens";
//        executeUpdateNamedQuery(namedQuery, new Param("current", current));
//    }
//
//    @Override
//    public void deleteToken(ForgotPwdDTO forgotPwdDTO) {
//        // TODO Auto-generated method stub
//        this.deleteObject(forgotPwdDTO);
//    }
//
//    @Override
//    public List<ForgotPwdDTO> listForgotPwdDTO(Timestamp current) {
//        String namedQuery = "pwd.listExpiredTokens";
//        return getObjectList(namedQuery, ForgotPwdDTO.class, "current", current);
//    }
//
//}
