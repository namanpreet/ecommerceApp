package com.qaddoo.persistence.dao.impl;

import com.mongodb.client.result.DeleteResult;
import com.qaddoo.persistence.dao.WallDAO;
import com.qaddoo.persistence.dto.WallDTO;
import com.qaddoo.persistence.dto.WallReactionDTO;
import com.qaddoo.pojo.entity.WallPostAttrs;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository("wallDAO")
public class WallDAOImpl implements WallDAO {

    private final MongoTemplate mongoTemplate;

    public WallDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public WallDTO getWallById(long wallId) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(wallId)), WallDTO.class);
    }

    @Override
    public WallDTO addWallPost(WallDTO wallDTO) {
        return mongoTemplate.insert(wallDTO);
    }

    @Override
    public WallDTO updateWallPost(WallDTO wallDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(wallDTO.getId())), wallDTO);
    }

    @Override
    public WallDTO updateLastActivity(Date timestamp, long wallId) {
        return mongoTemplate.findAndModify(new Query(Criteria.where("_id").is(wallId)),
                new Update().set("lastActivityOn", timestamp), WallDTO.class);
    }

    @Override
    public List<WallPostAttrs> listTanWalls(long tanId, long userId) {
        // stage to match walls by tanId
        AggregationOperation tanIdMatch = Aggregation.match(Criteria.where("tanId").is(tanId));
        // stage to fetch all like reactions on wall
        AggregationOperation wallReactionLookup = Aggregation.lookup("wall_reactions", "_id", "wallId", "reactions");
        // query to project required fields
        String projectQuery = "{$project: {\n" +
                "  \"_id\": 0,\n" +
                "  \"wallId\": \"$_id\",\n" +
                "  \"tanId\": 1,\n" +
                "  \"wallPostTitle\": 1,\n" +
                "  \"wallPostDescription\": 1,\n" +
                "  \"hasImage\": \"$wallPostHasBinaryData\",\n" +
                "  \"hasVideo\": \"$hasVideo\",\n" +
                "  \"imageName\": \"$imageName\",\n" +
                "  \"likeCount\": {$size:\"$reactions\"},\n" +
                "  \"isLiked\": {\n" +
                "    $cond: {\n" +
                "      if: {$in:[" + userId + ",\"$reactions.userId\"]},\n" +
                "      then: true,\n" +
                "      else: false\n" +
                "    }\n" +
                "  }\n" +
                "}}";

        // Pass the aggregation pipeline stages, in respective order, to build the aggregation query
        Aggregation aggregation = Aggregation.newAggregation(
                tanIdMatch,
                wallReactionLookup,
                new CustomProjectAggregationOperation(projectQuery)
        );

        return mongoTemplate.aggregate(aggregation, "walls", WallPostAttrs.class).getMappedResults();
    }

    @Override
    public void deleteWall(WallDTO wallDTO) {
        mongoTemplate.remove(wallDTO);
    }

    @Override
    public WallReactionDTO addWallReaction(WallReactionDTO wallReactionDTO) {
        return mongoTemplate.insert(wallReactionDTO);
    }

    @Override
    public DeleteResult removeWallReaction(long wallId, long userId) {
        return mongoTemplate.remove(new Query(Criteria.where("wallId").is(wallId)
                .andOperator(Criteria.where("userId").is(userId))), WallReactionDTO.class);
    }

    @Override
    public WallPostAttrs getWallPostById(long wallId, long userId) {
        AggregationOperation match = Aggregation.match(Criteria.where("_id").is(wallId));
        AggregationOperation likes = Aggregation.lookup("wall_reactions", "_id", "wallId", "wall_reaction");
        String project = " {\n" +
                "    $project: {\n" +
                "        wallId: '$_id',\n" +
                "        tanId: '$tanId',\n" +
                "        wallPostTitle: '$wallPostTitle',\n" +
                "        likeCount: {\n" +
                "            $cond: {\n" +
                "                'if': {\n" +
                "                    $isArray: '$wall_reaction'\n" +
                "                },\n" +
                "                then: {\n" +
                "                    $size: '$wall_reaction'\n" +
                "                },\n" +
                "                'else': 0\n" +
                "            }\n" +
                "        },\n" +
                "        isLiked: {\n" +
                "            $cond: {\n" +
                "                'if': {\n" +
                "                    $in: [\n" +
                "                        " + userId + ",\n" +
                "                        '$wall_reaction.userId'\n" +
                "                    ]\n" +
                "                },\n" +
                "                then: true,\n" +
                "                'else': false\n" +
                "            }\n" +
                "        },\n" +
                "        createdBy: '',\n" +
                "        imageName: '$imageName',\n" +
                "        hasVideo: '$hasVideo',\n" +
                "        hasImage: '$wallPostHasBinaryData',\n" +
                "        wallPostDescription: '$wallPostDescription'\n" +
                "    }\n" +
                "}";

        Aggregation aggregation = Aggregation.newAggregation(
                match,
                likes,
                new CustomProjectAggregationOperation(project)
        );
        return mongoTemplate.aggregate(aggregation, "walls", WallPostAttrs.class).getUniqueMappedResult();

    }

}
