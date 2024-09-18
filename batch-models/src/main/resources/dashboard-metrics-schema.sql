-- Create the 'dashboard' database
CREATE DATABASE dashboard;

-- Connect to the 'dashboard' database
\c dashboard;

-- Table: user_detail
CREATE TABLE dsr_metrics(
   id bigserial,
   created_date CURRENT_DATE,
   created_time CURRENT_TIME,
   central_ministries INTEGER,
   state_ut INTEGER,
   department_organisations_onboarded INTEGER,
   org_with_mdo_admin_leader_count INTEGER,
   org_with_mdo_admin_count INTEGER,
   org_with_live_course_count INTEGER,
   total_course_publishers INTEGER,
   total_courses_published INTEGER,
   total_courses_draft INTEGER,
   total_courses_review INTEGER,
   total_courses_retired INTEGER,
   total_courses_pending_published INTEGER,
   total_course_duration INTEGER,
   total_course_enrolments INTEGER,
   total_course_completions INTEGER,
   total_user_count INTEGER,
   new_user_registrations_yesterday INTEGER,
   user_logged_in_yesterday INTEGER,
   users_enrolled_in_at_least_one_course INTEGER
);