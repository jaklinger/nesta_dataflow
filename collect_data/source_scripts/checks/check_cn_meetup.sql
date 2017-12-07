use tier_0;

select 'Generating start groups' AS '';
CREATE TEMPORARY TABLE start_groups (select id from meetup_groups where country_name="United Kingdom" and category_id=34);

select 'Generating start events' AS '';
CREATE TEMPORARY TABLE start_events (select distinct(event_id) from meetup_groups_events where group_id in (select id from start_groups));

select 'Generating start members' AS '';
CREATE TEMPORARY TABLE start_members (select distinct(member_id) from meetup_events_members where event_id in (select event_id from start_events));

select 'Generating expanded groups' AS '';
CREATE TEMPORARY TABLE expanded_groups (select distinct(group_id) from meetup_groups_members where member_id in (select member_id from start_members));

select 'Generating expanded events' AS '';
CREATE TEMPORARY TABLE expanded_events (select distinct(event_id) from meetup_groups_events where group_id in (select group_id from expanded_groups));

-- Count base number of groups
select 'Base number of groups with initial category and country' AS '';
select count(*) from start_groups;

-- Count number of groups with an event
select 'Base number groups with events' AS '';
select count(distinct(group_id)) from meetup_groups_events where group_id in (select id from start_groups);

-- Count group event pairs
select 'Base number groups-event pairs' AS '';
select count(*) from start_events;

-- Count number of events with a member
select 'Base number of events with any member' AS '';
select count(distinct(event_id)) from meetup_events_members where event_id in (select event_id from start_events);

-- Count number of event member pairs
select 'Base number of events-member pairs' AS '';
select count(*) from meetup_events_members where event_id in (select event_id from start_events);

-- Count unique members attending events
select 'Base number of unique members at events' AS '';
select count(*) from start_members;

-- Count member group pairs
select 'Expanded number of members-group pairs' AS '';
select count(*) from meetup_groups_members where member_id in (select member_id from start_members);

-- Count expanded number of groups
select 'Expanded number of groups' AS '';
select count(group_id) from expanded_groups;

-- Count expanded number of groups with events
select 'Expanded number of groups with events' AS '';
select count(distinct(group_id)) from meetup_groups_events where group_id in (select group_id from expanded_groups);

select distinct(group_id) from meetup_groups_events where group_id in (select group_id from expanded_groups) limit 10;

-- Count expanded number of events
select 'Expanded number of events' AS '';
select count(event_id) from expanded_events;

-- 
-- select 'Expanded number of unique members-events' AS '';

-- Summary
select 'Expanded number of groups with info also collected' AS '';
select count(id) from meetup_groups where id in (select group_id from expanded_groups);



-- Tidy up
DROP TEMPORARY TABLE start_groups;
DROP TEMPORARY TABLE start_events;
DROP TEMPORARY TABLE start_members;
DROP TEMPORARY TABLE expanded_groups;
DROP TEMPORARY TABLE expanded_events;
