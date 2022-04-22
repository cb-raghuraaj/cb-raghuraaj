/*
 * Copyright (c) 2018 Chargebee Inc
 * All Rights Reserved
 */
package com.chargebee.app.scheduler.jobs.helpers;

import com.chargebee.app.application.api.MerchantCustomFields;
import com.chargebee.app.application.api.subscription.SubscriptionBizops;
import com.chargebee.app.application.api.utils.AppSubscriptionListHelper;
import com.chargebee.app.application.api.utils.ChargebeeAppListUtil;
import com.chargebee.app.bigquery.data.BQComparator;
import com.chargebee.app.bigquery.defn.BQCondition;
import com.chargebee.app.bigquery.defn.BQField;
import com.chargebee.app.bigquery.model.MerchantTpvTable;
import com.chargebee.app.bigquery.model.RevenueSegmentTable;
import com.chargebee.app.bigquery.request.BQQueries;
import com.chargebee.app.bigquery.util.BQMerchantTpvResult;
import com.chargebee.app.bigquery.util.BQRevenueSegmentResult;
import com.chargebee.app.bigquery.util.BigQuerySelectImpl;
import com.chargebee.app.churnzero.api.CZKey;
import com.chargebee.app.common.data.BizOpsIntegrations;
import com.chargebee.app.framework.model.ChargebeeSiteUsersConfig;
import com.chargebee.app.framework.util.CbSiteUsersUtil;
import com.chargebee.app.framework.util.ChargebeeAppUtil;
import com.chargebee.app.framework.util.ChargebeeUsersUtil;
import com.chargebee.app.framework.util.IntegHttpUtil;
import com.chargebee.app.hubspot.api.HubspotTask;
import com.chargebee.app.hubspot.model.HsSiteDataHelper;
import com.chargebee.app.hubspot.model.HubspotSite;
import com.chargebee.app.merchants.helpers.UnassignedContactsHelper;
import com.chargebee.app.models.Site;
import com.chargebee.app.models.SitesUser;
import com.chargebee.app.models.User;
import com.chargebee.app.models.base.SchJobBase;
import com.chargebee.app.scheduler.RecurringJob;
import com.chargebee.framework.env.Env;
import com.chargebee.framework.http.HttpConfig;
import com.chargebee.framework.http.util.HttpUtil;
import com.chargebee.framework.jooq.SqlUtils;
import com.chargebee.framework.metamodel.SegmentConfig;
import com.chargebee.framework.util.DateUtils;
import com.chargebee.framework.util.StringUtils;
import com.chargebee.framework.web.URLBuilder;
import com.chargebee.logging.KVL;
import com.chargebee.v2.models.Customer;
import org.jooq.Record;
import org.jooq.Result;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;

import static com.chargebee.app.application.api.utils.AppSubscriptionListHelper.getCouponName;
import static com.chargebee.app.bigquery.data.BQOperator.AND;
import static com.chargebee.app.models.CoreTables.qsites;

/**
 * @author cb-raghuraaj
 */
public abstract class PushSitesJobUtil extends RecurringJob {

    static int associationBatchCount = 0;
    private static final RevenueSegmentTable rst = new RevenueSegmentTable();
    private static final MerchantTpvTable mtt = new MerchantTpvTable();

    public static JSONObject frameRequestForBatchSitesGet(List<String> domainsList) throws Exception {
        JSONObject req = new JSONObject();
        JSONArray inpArray = frameRequestInputArray(domainsList);
        req.put("idProperty", "domain");
        req.put("inputs", inpArray);
        return req;
    }

    private static JSONArray frameRequestInputArray(List<String> domainsList) throws Exception {
        JSONArray inputArray = new JSONArray();
        for (String domain : domainsList) {
            inputArray.put(new JSONObject().put("id", domain));
        }
        return inputArray;
    }

    public static JSONObject frameRequestForSiteCreateOrUpdate(Site site, Map<String, Map<String, String>> siteVsWeeklyData, SubscriptionBizops sub, Customer cust, Map<String, String> couponIdVsName, String siteId) throws Exception {
        JSONObject request = new JSONObject();
        JSONObject properties = new JSONObject();
        processMerchantFields(properties, siteVsWeeklyData.get(site.id().toString()), cust, sub, couponIdVsName, site);
//        processBigQueryFields(properties);
        request.put("properties", properties);
        if (siteId != null) {
            request.put("id", siteId);
        }
        return request;
    }

    public static JSONObject frameRequestForBatchSitesCreate(List<JSONObject> sitesToBeCreated) throws Exception {
        JSONObject request = new JSONObject();
        JSONArray inputs = new JSONArray();
        for (JSONObject json : sitesToBeCreated) {
            inputs.put(json);
        }
        request.put("inputs", inputs);
        return request;
    }

    public static JSONObject frameRequestForBatchSitesUpdate(Map<String, JSONObject> sitesToBeUpdated) throws Exception {
        JSONObject request = new JSONObject();
        JSONArray inputs = new JSONArray();
        Iterator<JSONObject> iter = sitesToBeUpdated.values().iterator();
        while (iter.hasNext()) {
            inputs.put(iter.next());
        }
        request.put("inputs", inputs);
        return request;
    }

    public static JSONObject frameRequestForBatchSitesCreateOrUpdate(JSONArray sitesToBeCreatedOrUpdated) throws Exception {
        JSONObject request = new JSONObject();
        request.put("inputs", sitesToBeCreatedOrUpdated);
        return request;
    }

    public static JSONObject frameRequestForBatchSitesCreateOrUpdate(Map<String, JSONObject> sitesToBeCreatedOrUpdated) throws Exception {
        JSONObject request = new JSONObject();
        JSONArray inputs = new JSONArray();
        for (JSONObject json : sitesToBeCreatedOrUpdated.values()) {
            inputs.put(json);
        }
        request.put("inputs", inputs);
        return request;
    }

    public static JSONObject frameRequestForBatchAssociationsGet(List<String> hsContactIdsList) throws Exception {
        JSONObject req = new JSONObject();
        JSONArray inputs = new JSONArray();
        for (String hsContactId : hsContactIdsList) {
            inputs.put(new JSONObject().put("id", hsContactId));
        }
        req.put("inputs", inputs);
        return req;
    }

    public static Map<String, List<String>> getHsContactVsSitesAssociations(JSONObject obj) throws Exception {
        Map<String, List<String>> hsContactsVsSitesAssociations = new HashMap<>();
        if ("COMPLETE".equals(obj.getString("status")) && obj.has("results")) {
            JSONArray results = obj.getJSONArray("results");
            List<JSONObject> jsonObjectList = results.list();
            for (JSONObject jsonObject : jsonObjectList) {
                JSONArray toSites = jsonObject.getJSONArray("to");
                List<JSONObject> jsonList = toSites.list();
                List<String> siteIds = new ArrayList<>();
                for (JSONObject temp : jsonList) {
                    if ("site_to_contact".equals(temp.getString("type"))) {
                        siteIds.add(temp.getString("id"));
                    }
                }
                hsContactsVsSitesAssociations.put(jsonObject.getJSONObject("from").getString("id"), siteIds);
            }
        }
        return hsContactsVsSitesAssociations;
    }

    public static void frameRequestForSitesToAssociateOrDissociate(Map<String, List<String>> hsContactVsAssociationIds, boolean isAssociate, String hsContactId, JSONArray inputs) throws Exception {
        List<String> siteIds = hsContactVsAssociationIds.get(isAssociate ? "associate" : "dissociate");
        frameToJSON(siteIds, inputs, hsContactId, isAssociate);
    }

    public static JSONObject frameRequestForSitesToAssociateOrDissociate(Map<String, Map<String, List<String>>> hsContactVsAssociationIds, boolean isAssociate) throws Exception {
        JSONArray inputs = new JSONArray();
        Iterator<String> iter = hsContactVsAssociationIds.keySet().iterator();
        while (iter.hasNext()) {
            String hsContactId = iter.next();
            frameRequestForSitesToAssociateOrDissociate(hsContactVsAssociationIds.get(hsContactId), isAssociate, hsContactId, inputs);
        }
        return new JSONObject().put("inputs", inputs);
    }

    private static void frameToJSON(List<String> sitesIds, JSONArray inputs, String hsContactId, boolean isAssociate) throws Exception {
        JSONObject fromJson = new JSONObject().put("id", hsContactId);
        for (String siteId : sitesIds) {
            if (associationBatchCount > 2000) {
                KVL.put("batchCountExceededForAssociation", associationBatchCount);
            }
            JSONObject innerJSON = new JSONObject();
            innerJSON.put("type", isAssociate ? "site_to_contact" : "contact_to_site");
            innerJSON.put("from", fromJson);
            innerJSON.put("to", new JSONObject().put("id", siteId));
            inputs.put(innerJSON);
            associationBatchCount++;
        }
    }

    public static void populateHsContactVsAssociationIds(Map<String, List<String>> hubspotContactVsSiteDomains, Map<String, List<String>> hubspotContactVsSiteIds, Map<String, String> siteIdsVsDomains, Map<String, HubspotSite> newSitesCreatedInHS) throws Exception {
        Map<String, Map<String, List<String>>> remainingHsContactVsAssociationIds = new HashMap<>();
        Iterator<String> it = hubspotContactVsSiteDomains.keySet().iterator();
        int totalCount = 0;
        while (it.hasNext()) {
            List<String> siteIdsToAssociate = new ArrayList<>();
            List<String> siteIdsToDissociate = new ArrayList<>();
            Map<String, List<String>> associationMap = new HashMap<>();
            String hsContactId = it.next();
            List<String> currentDomains = hubspotContactVsSiteDomains.get(hsContactId);
            List<String> hubspotSiteIdsAssociatedInHubspotContact = hubspotContactVsSiteIds.get(hsContactId);
            if (hubspotSiteIdsAssociatedInHubspotContact != null) {
                int associationCount = 0;
                for (String siteIdInHubspot : hubspotSiteIdsAssociatedInHubspotContact) {
                    String domain = siteIdsVsDomains.get(siteIdInHubspot);
                    if (domain != null && !currentDomains.contains(domain)) {
                        siteIdsToDissociate.add(siteIdInHubspot);
                        currentDomains.remove(domain);
                        associationCount++;
                    }
                    if (associationCount == 2000) {
                        Map<String, Map<String, List<String>>> hsContactVsAssociationIds = new HashMap<>();
                        associationMap.put("dissociate", siteIdsToDissociate);
                        hsContactVsAssociationIds.put(hsContactId, associationMap);
                        HubspotTask.associateSitesToContactsBatch(PushSitesJobUtil.frameRequestForSitesToAssociateOrDissociate(hsContactVsAssociationIds, false));
                        totalCount += associationCount;
                        associationCount = 0;
                    }
                }
                if (associationCount > 0 && associationCount < 2000) {
                    associationMap.put("dissociate", siteIdsToDissociate);
                    remainingHsContactVsAssociationIds.put(hsContactId, associationMap);
                    totalCount += associationCount;
                }
            }
            if (currentDomains != null && newSitesCreatedInHS != null) {
                int associationCount = 0;
                for (String domainsToAssociate : currentDomains) {
                    HubspotSite hubspotSiteToAssociate = newSitesCreatedInHS.get(domainsToAssociate);
                    if (hubspotSiteToAssociate != null) {
                        siteIdsToAssociate.add(hubspotSiteToAssociate.getSiteId());
                        associationCount++;
                    }
                    if (associationCount == 2000) {
                        Map<String, Map<String, List<String>>> hsContactVsAssociationIds = new HashMap<>();
                        associationMap.put("associate", siteIdsToAssociate);
                        hsContactVsAssociationIds.put(hsContactId, associationMap);
                        HubspotTask.associateSitesToContactsBatch(PushSitesJobUtil.frameRequestForSitesToAssociateOrDissociate(hsContactVsAssociationIds, true));
                        totalCount += associationCount;
                        associationCount = 0;
                    }
                }
                if (associationCount > 0 && associationCount < 2000) {
                    associationMap.put("associate", siteIdsToAssociate);
                    remainingHsContactVsAssociationIds.put(hsContactId, associationMap);
                    totalCount += associationCount;
                }
            }
            if (totalCount > 2000) {
                KVL.put("batchCountExceededForAssociationWhilePopulating", totalCount);
            }
        }
        HubspotTask.dissociateSitesFromContactsBatch(PushSitesJobUtil.frameRequestForSitesToAssociateOrDissociate(remainingHsContactVsAssociationIds, false));
        HubspotTask.associateSitesToContactsBatch(PushSitesJobUtil.frameRequestForSitesToAssociateOrDissociate(remainingHsContactVsAssociationIds, true));
    }

    public static HashMap<String, Object> getAllLiveSitesByBatch(int batchSize, int offset) throws Exception {
        Callable<Result<Record>> callable = () -> {
            Result<Record> siteDetails = SqlUtils.Sql().select(qsites.ALL)
                    .from(qsites)
                    .where(qsites.sandbox.equal(false).and(qsites.domain.notIn("mannar", "merchant")))
                    .orderBy(qsites.id)
                    .limit(batchSize)
                    .offset(offset)
                    .fetch();
            return siteDetails;
        };
        return processSitesResult(SegmentConfig.invokeWithDisabledSegment(callable));
    }

    private static HashMap<String, Object> processSitesResult(Result<Record> sitesResult) {
        HashMap<String, Object> detailsMap = new HashMap<>();
        HashMap<String, String> siteIdVsDomains = new HashMap<>();
        HashSet<Site> allSitesInCurrentBatch = new HashSet<>();
        List<String> domainsList = new ArrayList<>();
        Integer fetchedCount = 0;
        for (Record rec : sitesResult) {
            Site site = qsites.create(rec);
            allSitesInCurrentBatch.add(site);
            domainsList.add(site.domain());
            siteIdVsDomains.put(site.id().toString(), site.domain());
            fetchedCount++;
        }
        detailsMap.put("allSitesInCurrentBatch", allSitesInCurrentBatch);
        detailsMap.put("domainsList", domainsList);
        detailsMap.put("siteIdVsDomains", siteIdVsDomains);
        detailsMap.put("fetchedCount", fetchedCount);
        return detailsMap;
    }

    public static void processBQFieldsForHSSites() throws Exception {
        JSONArray inputsToBeCreated = new JSONArray();
        Map<String, JSONObject> inputsToBeUpdated = new HashMap<>();
        Map<String, String> siteIdsVsDomains = new HashMap<>();
        Map<String, MerchantTpvTable> custIdVsBQ;
        Map<String, RevenueSegmentTable> custIdVsRS;
        Map<String,Customer> custIdVsCust;

        String query = getRevenueSegmentQueryForPastDay();
        BQRevenueSegmentResult revenueSegementResult = BQQueries.getRevenueSegementResult(query);
        if (revenueSegementResult != null && revenueSegementResult.totalRows > 0) {
            HashMap<Object,Object> detailsMap = getDetailsMap(revenueSegementResult);
            custIdVsCust = (Map<String, Customer>) detailsMap.get("custIdVsCust");
            custIdVsRS = (Map<String, RevenueSegmentTable>) detailsMap.get("custIdVsBQ");
            Map<String, HubspotSite> domainVsHsSite = HubspotTask.getSitesInBatchForDomain(PushSitesJobUtil.frameRequestForBatchSitesGet((List<String>) detailsMap.get("domainsList")));
            Iterator<String> iterator = custIdVsRS.keySet().iterator();
            while (iterator.hasNext()) {
                String customerId = iterator.next();
                Customer c = custIdVsCust.get(customerId);
                if(c != null) {
                    populateRequestArray(inputsToBeCreated, siteIdsVsDomains, inputsToBeUpdated, (Map<String, String>) detailsMap.get("emailVsDomains"), c, customerId, custIdVsRS, domainVsHsSite, true);
                }
            }
        }
        if (inputsToBeUpdated.size() > 0) {
            HubspotTask.updateSitesInBatch(PushSitesJobUtil.frameRequestForBatchSitesCreateOrUpdate(inputsToBeUpdated));
        }
        inputsToBeUpdated.clear();

        BQMerchantTpvResult merchantTpvResult = getRecords();
        if (merchantTpvResult != null && merchantTpvResult.totalRows > 0) {
            HashMap<Object,Object> detailsMap = getDetailsMap(revenueSegementResult);
            custIdVsBQ = (Map<String, MerchantTpvTable>) detailsMap.get("custIdVsBQ");
            custIdVsCust = (Map<String, Customer>) detailsMap.get("custIdVsCust");
            Map<String, HubspotSite> domainVsHsSite = HubspotTask.getSitesInBatchForDomain(PushSitesJobUtil.frameRequestForBatchSitesGet((List<String>) detailsMap.get("domainsList")));
            Iterator<String> iterator = custIdVsBQ.keySet().iterator();
            while (iterator.hasNext()) {
                String customerId = iterator.next();
                Customer c = custIdVsCust.get(customerId);
                if(c != null) {
                    populateRequestArray(inputsToBeCreated, siteIdsVsDomains, inputsToBeUpdated, (Map<String, String>) detailsMap.get("emailVsDomains"), c, customerId, custIdVsBQ, domainVsHsSite, false);
                }
            }
        }
        if (inputsToBeUpdated.size() > 0) {
            HubspotTask.updateSitesInBatch(PushSitesJobUtil.frameRequestForBatchSitesCreateOrUpdate(inputsToBeUpdated));
        }
        if (inputsToBeCreated.length() > 0) {
            HubspotTask.createSitesInBatch(PushSitesJobUtil.frameRequestForBatchSitesCreateOrUpdate(inputsToBeCreated));
        }
    }

    private static void populateRequestArray(JSONArray inputsToBeCreated, Map<String, String> siteIdsVsDomains, Map<String, JSONObject> inputsToBeUpdated, Map<String, String> emailVsDomains, Customer c, String customerId, Map custIdVsResult, Map<String, HubspotSite> domainVsHsSite, boolean isRS) throws Exception{
        String domain = emailVsDomains.get(c.email());
        JSONObject request = new JSONObject();
        JSONObject properties = new JSONObject();
        properties.put("customer_id", customerId);
        properties.put("domain", domain);
        if(isRS){
            request.put("properties", frameBQFieldPropertiesObject((RevenueSegmentTable) custIdVsResult.get(customerId), properties));
        }else{
            request.put("properties", frameBQFieldPropertiesObjectForTPV((MerchantTpvTable) custIdVsResult.get(customerId), properties));
        }
        if (domainVsHsSite.get(domain) != null) {
            String id = domainVsHsSite.get(domain).getSiteId();
            request.put("id", id);
            inputsToBeUpdated.put(id, request);
            siteIdsVsDomains.put(id,domain);
        } else {
            inputsToBeCreated.put(request);
        }
    }

    public static HashMap<Object, Object> getDetailsMap(Object bqResults) throws Exception {
        HashMap<Object,Object> detailsMap = new HashMap<>();
        String[] customerIds = new String[Math.toIntExact(bqResults instanceof BQRevenueSegmentResult ? ((BQRevenueSegmentResult)bqResults).totalRows : ((BQMerchantTpvResult)bqResults).totalRows)];
        int i = 0;
        Map<String, Object> custIdVsBQ = new HashMap<>();
        if(bqResults instanceof BQRevenueSegmentResult) {
            for (RevenueSegmentTable rs : ((BQRevenueSegmentResult) bqResults).revenueSegment) {
                customerIds[i++] = rs.getMerchantId();
                custIdVsBQ.put(rs.getMerchantId(), rs);
            }
        }else{
            for (MerchantTpvTable rs : ((BQMerchantTpvResult) bqResults).merchantTpv) {
                customerIds[i++] = rs.getMerchantId();
                custIdVsBQ.put(rs.getMerchantId(), rs);
            }
        }
        Map<String, Customer> custIdVsCustomer = ChargebeeAppListUtil.getCustomerList(customerIds);
        String[] emailsArray = new String[custIdVsCustomer.size()];
        List<String> emailsList = new ArrayList<>();
        Iterator<String> it = custIdVsCustomer.keySet().iterator();
        int j = 0;
        while (it.hasNext()) {
            Customer c = custIdVsCustomer.get(it.next());
            emailsArray[j++] = c.email();
            emailsList.add(c.email());
        }
        Map<String, JSONObject> hsContacts = HubspotTask.getHsContactsUtil(emailsList, true).getContacts();
        Map<String, List<String>> hubspotContactVsSiteDomains = new HashMap<>();
        List<String> hsContactIdsList = new ArrayList<>();
        ChargebeeSiteUsersConfig usersConfig = new ChargebeeSiteUsersConfig(0L, null, true, true)
                .isLiveSiteOnly(true)
                .isActiveUsersOnly(false)
                .forEmails(emailsArray);
        CbSiteUsersUtil suUtil = usersConfig.getSiteUsers(null, true).couponResNeeded(true);
        HashMap<Long, Site> siteIdVsSite = suUtil.getSiteIdVssite();
        List<User> users = suUtil.getUsers();
        List<String> domainsList = new ArrayList<>();
        Map<String, String> emailVsDomains = new HashMap<>();
        for (User user : users) {
            HashSet<SitesUser> sitesUsers = suUtil.getSiteUsers(user.id());
            JSONObject hsContact = hsContacts.get(user.email().toLowerCase(Locale.ROOT));
            Iterator<SitesUser> iter = sitesUsers.iterator();
            while (iter.hasNext()) {// to get all domains to get hs sites
                SitesUser site = iter.next();
                domainsList.add(siteIdVsSite.get(site.siteId()).domain());
                emailVsDomains.put(user.email().toLowerCase(Locale.ROOT), siteIdVsSite.get(site.siteId()).domain());
            }
            if(hsContact != null){
                hsContactIdsList.add(hsContact.get("vid").toString());// to get all current site associations in hs for the contact ids in this list
                hubspotContactVsSiteDomains.put(hsContact.get("vid").toString(), domainsList);
            }
        }
        detailsMap.put("domainsList", domainsList);
        detailsMap.put("custIdVsBQ", custIdVsBQ);
        detailsMap.put("custIdVsCust", custIdVsCustomer);
        detailsMap.put("emailVsDomains", emailVsDomains);
        return detailsMap;
    }
    public static BQMerchantTpvResult getRecords() throws Exception {
        String query = getMerchantTPVQueryForPastDay();
        BQMerchantTpvResult mtr = BQQueries.getMerchantTPVResult(query);
        return mtr;
    }

    private static JSONObject frameBQFieldPropertiesObject(RevenueSegmentTable rs, JSONObject jsonObject) throws Exception {
        if (rs != null) {
            jsonObject.put("geography", rs.getRegion());
            jsonObject.put("industry", rs.getIndustry());
            jsonObject.put("vertical", rs.getVertical());
            jsonObject.put("segment", rs.getBusinessSize());
            jsonObject.put("business_type", rs.getBusinessModel());
        }
        return jsonObject;
    }

    private static JSONObject frameBQFieldPropertiesObjectForTPV(MerchantTpvTable mtpv, JSONObject jsonObject) throws Exception {
        if (mtpv != null) {
            if (!ChargebeeAppUtil.isValidCustomerHandle(mtpv.getMerchantId())) {
                return null;
            }
            jsonObject.put("tpv_buckets", mtpv.getLifeTimeTPV());
        }
        return jsonObject;
    }

    private static String getRevenueSegmentQueryForPastDay() {
        return new BigQuerySelectImpl()
                .from(rst.tableName)
                .where(new BQCondition(rst.UPDATED_AT.name(), dateCondition(DateUtils.startOfYesterday().toString(), DateUtils.startOfToday().toString()), BQComparator.BETWEEN, true))
                .getSelectQuery();
    }

    public static String getMerchantTPVQueryForPastDay() {
        return new BigQuerySelectImpl()
                .from(mtt.tableName)
                .where(new BQCondition(mtt.UPDATED_AT.name(), dateCondition(DateUtils.startOfYesterday().toString(), DateUtils.startOfToday().toString()), BQComparator.BETWEEN, true))
                .getSelectQuery();
    }

    public static String dateCondition(String startDateStr, String endDateStr) {
        BQField startTermField = new BQField(startDateStr);
        BQField endTermField = new BQField(endDateStr);
        return startTermField.addQuote().concat(AND.toSQL()).concat(endTermField.addQuote());
    }

    public abstract SchJobBase.JobType getJobType();

    public Integer batchLimit() {
        return ChargebeeUsersUtil.getCommonBatchLimit();
    }


    public static void addPreLogs(Timestamp schAt, Long startId) {
        KVL.put("hubspot.sites.scheduled_at", schAt);
        KVL.put("hubspot.sites.start_id", startId);
    }

    public static void addPostLogs(Timestamp nextSch, boolean isWithinBatch) {
        KVL.put("hubspot.sites.is_resh_within_batch", isWithinBatch);
        KVL.put("hubspot.sites.next_sch_at", nextSch);
    }

    public static int getAssociationTypeFromResponse(JSONObject resp) throws Exception {
        JSONObject result = (JSONObject) resp.optJSONArray("results").get(0);
        return result.optInt("id", -1);
    }

    public static JSONObject frameRequestJSON(String domain) throws Exception {
        JSONObject request = new JSONObject();
        JSONArray filterGroups = new JSONArray();
        JSONArray filters = new JSONArray();
        JSONObject filter_outer = new JSONObject();
        JSONObject filter_inner = new JSONObject();
        filter_inner.put("propertyName", "domain");
        filter_inner.put("operator", "EQ");
        filter_inner.put("value", domain);
        filters.put(filter_inner);
        filter_outer.put("filters", filters);
        filterGroups.put(filter_outer);
        request.put("filterGroups", filterGroups);
        return request;
    }

    public static void updateSite(Customer c, String siteId) throws Exception {
        JSONObject request = new JSONObject();
        JSONObject properties = new JSONObject();
        if (c != null && c.optString(MerchantCustomFields.csm) != null) {
            String csm = c.optString(MerchantCustomFields.csm);
            properties.put("ces_csm", csm);
            String csmEmail = UnassignedContactsHelper.getEmailForName(csm);
            Object calendlyLink = getCalendlyLinkFromChurnZero(csmEmail);
            if (calendlyLink != null) {
                properties.put("ces_csm_calendly", calendlyLink.toString());
            }
            properties.put("ces_csm_email", StringUtils.isEmpty(csmEmail) ? "" : csmEmail.replace("@", "@hey."));
        }
        properties.put("customer_id", c.id());
        request.put("properties", properties);
        HubspotTask.updateSite(request, siteId);
    }

    public static void updateSite(SubscriptionBizops subscriptionBizops, Customer c, String siteId) throws Exception {
        JSONObject request = new JSONObject();
        JSONObject properties = new JSONObject();
        properties.put("subscription_status", subscriptionBizops.status().name());
        properties.put("plan_name", ChargebeeAppUtil.getPlan(subscriptionBizops.planId()).name());
        if (c != null && c.optString(MerchantCustomFields.csm) != null) {
            String csm = c.optString(MerchantCustomFields.csm);
            properties.put("ces_csm", csm);
            String csmEmail = UnassignedContactsHelper.getEmailForName(csm);
            properties.put("ces_csm_email", StringUtils.isEmpty(csmEmail) ? "" : csmEmail.replace("@", "@hey."));
        }
        properties.put("customer_id", c.id());
        request.put("properties", properties);
        HubspotTask.updateSite(request, siteId);
    }

    public static void processMerchantFields(JSONObject properties, Map<String, String> siteData, Customer c, SubscriptionBizops subscriptionBizops, Map<String, String> couponIdVsName, Site site) throws Exception {
        if (c != null && c.optString(MerchantCustomFields.csm) != null) {
            String csm = c.optString(MerchantCustomFields.csm);
            properties.put("ces_csm", csm);
            String csmEmail = UnassignedContactsHelper.getEmailForName(csm);
            Object calendlyLink = getCalendlyLinkFromChurnZero(csmEmail);
            if (calendlyLink != null) {
                properties.put("ces_csm_calendly", calendlyLink.toString());
            }
            properties.put("ces_csm_email", StringUtils.isEmpty(csmEmail) ? "" : csmEmail.replace("@", "@hey."));
        }
        properties.put("customer_id", "mer_" + site.merchantId());
        properties.put("domain", site.domain());
        if (siteData != null) {
            properties.put("gateways_configured", siteData.get(HsSiteDataHelper.HSSiteData.gwsConfigured.hsField()) == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.gwsConfigured.hsField()));
            properties.put("currencies_enabled", siteData.get(HsSiteDataHelper.HSSiteData.currenciesConfigured.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.currenciesConfigured.hsField())));
            properties.put("active_languages", siteData.get(HsSiteDataHelper.HSSiteData.languagesConfigured.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.languagesConfigured.hsField())));
            properties.put("site_tz", siteData.get(HsSiteDataHelper.HSSiteData.siteTimeZone.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.siteTimeZone.hsField())));
            properties.put("is_3ds_enabled", siteData.get(HsSiteDataHelper.HSSiteData.is3DSEnabled.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.is3DSEnabled.hsField())));
            properties.put("is_rs_enabled", siteData.get(HsSiteDataHelper.HSSiteData.isRevenueStoryEnabled.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.isRevenueStoryEnabled.hsField())));
            properties.put("integrations_enabled", siteData.get(HsSiteDataHelper.HSSiteData.integsEnabled.hsField() == null ? "" : siteData.get(HsSiteDataHelper.HSSiteData.integsEnabled.hsField())));
        }
        if (subscriptionBizops != null) {
            properties.put("subscription_status", subscriptionBizops.status().name());
            properties.put("plan_name", ChargebeeAppUtil.getPlan(subscriptionBizops.planId()).name());
            if (couponIdVsName != null && !couponIdVsName.isEmpty()) {
                properties.put("coupons", getCouponName(subscriptionBizops, couponIdVsName));
            }
            properties.put("addons", AppSubscriptionListHelper.getAddons(subscriptionBizops));
        }
    }

    private static Object getCalendlyLinkFromChurnZero(String csmEmail) throws Exception {
        HttpUtil http = new HttpUtil(new HttpConfig(null, null, "application/json", "application/json;charset=utf-8"));
        IntegHttpUtil httpUtil = new IntegHttpUtil(BizOpsIntegrations.CHURNZERO, http, Env.prop("churnzero.endpoint", "https://analytics.churnzero.net/i"));
        Object calendlyLink = null;
        JSONObject jsonObject = new JSONObject(httpUtil.getRequest(getRequestPathForCZCalendlyLink(csmEmail), Base64.getEncoder().encodeToString("Raghu Raaj:1!kURKvETlQrXzRseIfdkm9r67kSZpxw4ebdfPJ-bNm7SAzK3eC1IGGAK7pAt532".getBytes(StandardCharsets.UTF_8))));
        if (jsonObject.has("value") && jsonObject.getJSONArray("value").length() > 0) {
            JSONObject userAccount = jsonObject.getJSONArray("value").getJSONObject(0);
            calendlyLink = userAccount.getJSONObject("Cf").get("CalendlyLink");
        }
        return calendlyLink;
    }

    private static String getRequestPathForCZCalendlyLink(String csmEmail) {
        URLBuilder url = new URLBuilder()
                .path("/UserAccount?" + CZKey.FILTER_QP + getFilterQueryParam(csmEmail));
        return url.gen();
    }

    private static String getFilterQueryParam(String csmEmail) {
        return "=Email+eq+'" + csmEmail + "'" + "&$select=Cf";
    }

    public static Integer nextPeriod() {
        return Env.v("hubspot.push_sites.job.batch_interval.minutes", 20, 3);
    }

    public static Integer nextBatch() {
        return Env.v("hubspot.push_sites.job.batch_interval.minutes", 1, 1);
    }

    public static Integer batchSize() {
        return Env.v("hubspot.push_sites.job.batch_size", 100, 200);
    }

    public static Boolean isSiteSchemaCreated() {
        return Env.bool("hubspot.site.custom_object.schema.created", true);
    }
}
