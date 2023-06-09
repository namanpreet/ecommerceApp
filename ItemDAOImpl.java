package com.qaddoo.persistence.dao.impl;

import com.google.common.base.Strings;
import com.qaddoo.persistence.PersistenceUtils;
import com.qaddoo.persistence.dao.ItemDAO;
import com.qaddoo.persistence.dto.ItemDTO;
import com.qaddoo.pojo.entity.ItemAttrs;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class ItemDAOImpl implements ItemDAO {

    private final MongoTemplate mongoTemplate;

    public ItemDAOImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public ItemDTO addItem(ItemDTO itemDTO) {
        return mongoTemplate.insert(itemDTO);
    }

    @Override
    public ItemDTO getItemById(long id) {
        return mongoTemplate.findOne(new Query(Criteria.where("_id").is(id)), ItemDTO.class);
    }

    @Override
    public ItemDTO getItemByName(String name) {
        name = new PersistenceUtils().addBackslash(name);
        return mongoTemplate.findOne(new Query(new Criteria().orOperator(
                Criteria.where("english").regex("^" + name + "$", "i"),
                Criteria.where("hindi").regex("^" + name + "$", "i"),
                Criteria.where("marathi").regex("^" + name + "$", "i")
        )), ItemDTO.class);
    }

    @Override
    public boolean doesItemExist(String name) {
        name = new PersistenceUtils().addBackslash(name);
        return mongoTemplate.exists(new Query(new Criteria().orOperator(
                Criteria.where("english").regex("^" + name + "$", "i"),
                Criteria.where("hindi").regex("^" + name + "$", "i"),
                Criteria.where("marathi").regex("^" + name + "$", "i")
        )), ItemDTO.class);
    }

    @Override
    public ItemDTO updateItem(ItemDTO itemDTO) {
        return mongoTemplate.findAndReplace(new Query(Criteria.where("_id").is(itemDTO.getId())), itemDTO);
    }

    @Override
    public ItemDTO updatePresentInList(long itemId, long tanId) {
        Query query = new Query(Criteria.where("_id").is(itemId));
        Map<String, Object> tan = new HashMap<>();
        tan.put("$id", tanId);
        tan.put("$ref", "tans");
        UpdateDefinition updateDefinition = new Update().push("presentIn", tan);
        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        return mongoTemplate.findAndModify(query, updateDefinition, options, ItemDTO.class);
    }

    @Override
    public ItemDTO updateItemImage(long itemId, boolean hasImage, String imageName) {
        Query query = new Query(Criteria.where("_id").is(itemId));
        UpdateDefinition updateDefinition;
        if (hasImage) {
            updateDefinition = new Update().set("hasImage", true).set("imageName", imageName);
        } else {
            updateDefinition = new Update().set("hasImage", false);
        }
        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        return mongoTemplate.findAndModify(query, updateDefinition, options, ItemDTO.class);
    }

    @Override
    public List<ItemDTO> listItemsWithNoCountry() {
        return mongoTemplate.find(new Query(Criteria.where("countries").is(null)), ItemDTO.class);
    }

    @Override
    public List<ItemDTO> listItemsByCategory(long categoryId) {
        return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)), ItemDTO.class);
    }

    @Override
    public long itemCountByCategory(long categoryId) {
        return mongoTemplate.count(new Query(Criteria.where("category").is(categoryId)), ItemDTO.class);
    }

    // fetch products according to the arguments, sorted by products containing image at the front
    @Override
    public List<ItemDTO> listItems(long categoryId, String subCategory, String language, String name, String country, int page) {
        Pageable pageable = PageRequest.of(page, 30);

        if (!Strings.isNullOrEmpty(country)) {
            if (!Strings.isNullOrEmpty(name)) {
                name = new PersistenceUtils().addBackslash(name);
                return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)
                        .andOperator(Criteria.where("countries").all(country))
                        .andOperator(Criteria.where(language).regex(".*" + name + ".*", "i")))
                        .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
            } else if (!Strings.isNullOrEmpty(subCategory)) {
                subCategory = new PersistenceUtils().addBackslash(subCategory);
                return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)
                        .andOperator(Criteria.where("countries").all(country))
                        .andOperator(Criteria.where("subcategory").regex("^" + subCategory + "$", "i")))
                        .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
            } else {
                return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)
                        .andOperator(Criteria.where("countries").all(country)))
                        .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
            }
        }

        if (!Strings.isNullOrEmpty(name)) {
            name = new PersistenceUtils().addBackslash(name);
            return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)
                    .andOperator(Criteria.where(language).regex(".*" + name + ".*", "i")))
                    .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
        } else if (!Strings.isNullOrEmpty(subCategory)) {
            subCategory = new PersistenceUtils().addBackslash(subCategory);
            return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId)
                    .andOperator(Criteria.where("subcategory").regex("^" + subCategory + "$", "i")))
                    .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
        } else {
            return mongoTemplate.find(new Query(Criteria.where("category").is(categoryId))
                    .with(Sort.by(Sort.Direction.DESC, "hasImage")).with(pageable), ItemDTO.class);
        }

    }

    @Override
    public List<ItemAttrs> searchItemsByName(String keyword, int page) {
        keyword = new PersistenceUtils().addBackslash(keyword);
        Pageable pageable = PageRequest.of(page, 30);
        // query to match english or hindi name against the keyword
        String matchQuery = "{$match: {\n" +
                "  $and:[\n" +
                "    {\n" +
                "      $and:[\n" +
                "        {presentIn:{$ne:null}},\n" +
                "        {presentIn:{$ne:[]}}\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      $or:[\n" +
                "        {english:RegExp('" + keyword + "', 'i')},\n" +
                "        {hindi:RegExp('" + keyword + "', 'i')}\n" +
                "        {marathi:RegExp('" + keyword + "', 'i')}\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}}";
        // query to add field "presentInLength" and "hindiLang"
        String addFieldQuery = "{$addFields: {\n" +
                "  presentInLength: {$size:\"$presentIn\"},\n" +
                "  matchedLanguage: {\n" +
                "    $switch: {\n" +
                "      branches: [\n" +
                "        {\n" +
                "          case: {$regexMatch:{input:\"$marathi\", regex:/" + keyword + "/i}},\n" +
                "          then: \"marathi\"\n" +
                "        },\n" +
                "        {\n" +
                "          case: {$regexMatch:{input:\"$hindi\", regex:/" + keyword + "/i}},\n" +
                "          then: \"hindi\"\n" +
                "        }\n" +
                "      ],\n" +
                "      default: \"english\"\n" +
                "    }\n" +
                "  }\n" +
                "}}";
        // stage to sort according to number of stores present in
        AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "presentInLength");
        // stage to limit the results
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize() +
                ((long) pageable.getPageNumber() * pageable.getPageSize()));
        AggregationOperation skip = Aggregation.skip((long) pageable.getPageNumber() * pageable.getPageSize());
        // query to project final fields
        String project = "{$project: {\n" +
                "  _id: 0,\n" +
                "  name: {\n" +
                "    $switch: {\n" +
                "      branches: [\n" +
                "        {\n" +
                "          case: {$eq:[\"$matchedLanguage\",\"marathi\"]},\n" +
                "          then: \"$marathi\"\n" +
                "        },\n" +
                "        {\n" +
                "          case: {$eq:[\"$matchedLanguage\",\"hindi\"]},\n" +
                "          then: \"$hindi\"\n" +
                "        }\n" +
                "      ],\n" +
                "      default: \"$english\"\n" +
                "    }\n" +
                "  },\n" +
                "  hasImage: 1,\n" +
                "  imageName: 1\n" +
                "}}";

        Aggregation aggregation = Aggregation.newAggregation(
                new CustomProjectAggregationOperation(matchQuery),
                new CustomProjectAggregationOperation(addFieldQuery),
                sort,
                limit,
                skip,
                new CustomProjectAggregationOperation(project)
        );

        return mongoTemplate.aggregate(aggregation, "items", ItemAttrs.class).getMappedResults();
    }

    // get all database items with pagination
    @Override
    public List<ItemDTO> getAllItemsWithoutLanguage(int page, String language) {
        Pageable pageable = PageRequest.of(page, 1000);

        // stage to filter the items which do not have the language name
        AggregationOperation match = Aggregation.match(Criteria.where("marathi").is(null));
        // stage to limit results
        AggregationOperation limit = Aggregation.limit(pageable.getPageSize());

        Aggregation aggregation = Aggregation.newAggregation(match, limit);

        return mongoTemplate.aggregate(aggregation, "items", ItemDTO.class).getMappedResults();
    }

    // set name of the item for a particular language
    @Override
    public void setLanguageNameAndDescription(String language, String name, String description, long itemId) {
        Query query = new Query(Criteria.where("_id").is(itemId));
        UpdateDefinition updateDefinition = new Update().set(language, name).set(language+"Description", description);
        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        mongoTemplate.findAndModify(query, updateDefinition, options, ItemDTO.class);
    }

}
