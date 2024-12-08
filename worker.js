const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  return await processHubspotEntities({
    domain,
    hubId,
    entityType: 'companies',
    lastPulledDateKey: 'companies',
    searchApiFunction: hubspotClient.crm.companies.searchApi,
    properties: [
      'name',
      'domain',
      'country',
      'industry',
      'description',
      'annualrevenue',
      'numberofemployees',
      'hs_lead_status'
    ],
    generateActionTemplate: (company, isCreated) => {
      if (!company.properties) return null;
      return {
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };
    },
    handleAssociations: null,
    q
  });
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  return await processHubspotEntities({
    domain,
    hubId,
    entityType: 'contacts',
    lastPulledDateKey: 'contacts',
    searchApiFunction: hubspotClient.crm.contacts.searchApi,
    properties: [
      'firstname',
      'lastname',
      'jobtitle',
      'email',
      'hubspotscore',
      'hs_lead_status',
      'hs_analytics_source',
      'hs_latest_source'
    ],
    generateActionTemplate: (contact, isCreated, associations) => {
      if (!contact.properties || !contact.properties.email) return null;

      const companyId = associations[contact.id];

      const userProperties = {
        company_id: companyId,
        contact_name: `${contact.properties.firstname || ''} ${contact.properties.lastname || ''}`.trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      return {
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };
    },
    handleAssociations: async data => {
      const contactIds = data.map(contact => contact.id);
      const companyAssociationsResults = await getAssociations(contactIds, 'CONTACTS', 'COMPANIES');
      return Object.fromEntries(companyAssociationsResults.map(a => {
        if (a.from) {
          contactIds.splice(contactIds.indexOf(a.from.id), 1);
          return [a.from.id, a.to[0].id];
        } else return false;
      }).filter(x => x));
    },
    q
  });
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  return await processHubspotEntities({
    domain,
    hubId,
    entityType: 'meetings',
    lastPulledDateKey: 'meetings',
    searchApiFunction: hubspotClient.crm.objects.meetings.searchApi,
    properties: [
      'hs_meeting_title',
      'hs_meeting_start_time',
      'hs_meeting_end_time',
      'hs_meeting_created_at',
    ],
    generateActionTemplate: (meeting, isCreated, associations) => {
      if (!meeting.properties || !meeting.properties.hs_meeting_title) return null;
      const email = associations.find(assoc => assoc.meetingId === meeting.id)?.contact?.email;
      return {
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.properties.hs_meeting_created_at : meeting.updatedAt),
        meetingProperties: {
          meeting_id: meeting.id,
          meeting_title: meeting.properties.hs_meeting_title,
          start_time: meeting.properties.hs_meeting_start_time,
          end_time: meeting.properties.hs_meeting_end_time,
          contact_email: email,
        }
      };
    },
    handleAssociations: async data => {
      const meetingIds = data.map(meeting => meeting.id);
      const contactAssociationsResults = await getAssociations(meetingIds, 'MEETINGS', 'CONTACTS');

      const associationMap = contactAssociationsResults.map((result) => ({
        meetingId: result.from.id,
        contactId: result.to.map((assoc) => assoc.id)[0],
      }));

      const contactIds = [...new Set(associationMap.map((assoc) => assoc.contactId))];

      let combinedResults = [];
      if (contactIds.length > 0) {
        const contactDetailsResponse = await hubspotClient.apiRequest({
          method: 'post',
          path: `/crm/v3/objects/contacts/batch/read`,
          body: {
            properties: ['id', 'email'],
            inputs: contactIds.map(id => id),
          },
        });

        const contactDetails = (await contactDetailsResponse.json())?.results || [];

        const contactMap = Object.fromEntries(
            contactDetails.map((contact) => [contact.id, contact.properties])
        );

        combinedResults = associationMap.map((assoc) => ({
          meetingId: assoc.meetingId,
          contact: contactMap[assoc.contactId] || null,
        }));
      }

      return combinedResults;
    },
    q
  });
};

const processHubspotEntities = async ({
                                        domain,
                                        hubId,
                                        entityType,
                                        lastPulledDateKey,
                                        searchApiFunction,
                                        properties,
                                        generateActionTemplate,
                                        handleAssociations,
                                        q
                                      }) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates[lastPulledDateKey] || 0);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties,
      limit,
      after: offsetObject.after,
    };

    let searchResult = {};
    let tryCount = 0;

    while (tryCount <= 4) {
      try {
        searchResult = await searchApiFunction.doSearch(searchObject);
        break;
      } catch (err) {
        console.log(err);
        tryCount++;
        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
        await new Promise(resolve => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error(`Failed to fetch ${entityType} for the 4th time. Aborting.`);

    const data = searchResult.results || [];
    offsetObject.after = parseInt(searchResult.paging?.next?.after);

    let associations = [];
    if (handleAssociations) {
      associations = await handleAssociations(data);
    }

    data.forEach(item => {
      const isCreated = !lastPulledDate || new Date(item.createdAt) > lastPulledDate;
      const actionTemplate = generateActionTemplate(item, isCreated, associations);

      if (actionTemplate) {
        q.push(actionTemplate);
      }
    });

    if (!offsetObject?.after) {
      hasMore = false;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates[lastPulledDateKey] = now;
  await saveDomain(domain);

  return true;
};

const getAssociations = async (inputs, objectType, associatedObjectType) => {
  const associationResponse = await hubspotClient.apiRequest({
    method: 'post',
    path: `/crm/v3/associations/${objectType}/${associatedObjectType}/batch/read`,
    body: { inputs },
  });

  return (await associationResponse.json())?.results || [];
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
