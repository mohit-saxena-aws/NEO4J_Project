///////////////////////////////////////////////////////////////////////////
// AURA-READY MASTER LOADER FOR COMPLETE GRAPH DB
// Replace the placeholder URLs with your S3 / GCS / HTTPS CSV links.
// This script is idempotent and safe to re-run.
// Optimized for Aura using LOAD CSV + PERIODIC COMMIT.
///////////////////////////////////////////////////////////////////////////


///////////////////////////////
// 1. UNIQUE CONSTRAINTS
///////////////////////////////

CREATE CONSTRAINT employee_id IF NOT EXISTS
FOR (e:Employee) REQUIRE e.employeeId IS UNIQUE;

CREATE CONSTRAINT job_id IF NOT EXISTS
FOR (j:Job) REQUIRE j.jobId IS UNIQUE;

CREATE CONSTRAINT location_id IF NOT EXISTS
FOR (l:Location) REQUIRE l.locationId IS UNIQUE;

CREATE CONSTRAINT skill_id IF NOT EXISTS
FOR (s:Skill) REQUIRE s.skillId IS UNIQUE;



///////////////////////////////
// 2. LOAD NODE FILES
///////////////////////////////

// -------- Locations --------
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/locations.csv' AS row
MERGE (loc:Location {locationId: row.locationId})
SET  loc.officeName = row.officeName,
     loc.city       = row.city,
     loc.state      = row.state,
     loc.country    = row.country,
     loc.region     = row.region,
     loc.timezone   = row.timezone,
     loc.latitude   = CASE WHEN row.latitude IS NULL  OR row.latitude = '' THEN null ELSE toFloat(row.latitude) END,
     loc.longitude  = CASE WHEN row.longitude IS NULL OR row.longitude = '' THEN null ELSE toFloat(row.longitude) END;


// -------- Skills --------
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/skills.csv' AS row
MERGE (s:Skill {skillId: row.skillId})
SET  s.name        = row.name,
     s.category    = row.category,
     s.description = coalesce(row.description,'');


// -------- Jobs --------
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/jobs.csv' AS row
MERGE (j:Job {jobId: row.jobId})
SET  j.title      = row.title,
     j.department = coalesce(row.department,''),
     j.minSalary  = CASE WHEN row.minSalary IS NULL OR row.minSalary = '' THEN null ELSE toInteger(row.minSalary) END,
     j.maxSalary  = CASE WHEN row.maxSalary IS NULL OR row.maxSalary = '' THEN null ELSE toInteger(row.maxSalary) END;


// -------- Employees (LARGE FILE) --------
USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/employees.csv' AS row
MERGE (e:Employee {employeeId: row.employeeId})
SET  e.firstName  = row.firstName,
     e.lastName   = row.lastName,
     e.email      = row.email,
     e.title      = coalesce(row.title,''),
     e.jobLevel   = coalesce(row.jobLevel,''),
     e.locationId = coalesce(row.locationId,'');

USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/employees.csv' AS row
MERGE (e:Employee {employeeId: row.employeeId})
SET  e.employeeId: row.employeeId,
     e.firstName: row.firstName,
     e.lastName: row.lastName,
     e.displayName: row.displayName,
     e.preferredName: row.preferredName,
     e.email: row.email,
     e.workPhone: row.workPhone,
     e.personalPhone: row.personalPhone,
     e.hireDate: date(row.hireDate),
     e.birthDate: date(row.birthDate),
     e.title: coalesce(row.title),
     e.jobLevel: coalesce(row.jobLevel),
     e.employmentType: row.employmentType,
     e.status: row.status,
     e.experienceYears: row.experienceYears,
     e.salaryAnnual: row.salaryAnnual,
     e.salaryCurrency: row.salaryCurrency,
     e.nationality: row.nationality,
     e.locationId: coalesce(row.locationId,''),
     e.languages: row.languages,
     e.workModel: row.workModel,
     e.linkedin: row.linkedin,
     e.managerEmployeeId: row.managerEmployeeId,
     e.hrEmployeeNumber: row.hrEmployeeNumber,
     e.officeBadge: row.officeBadge,
     e.createdAt: datetime(),
     e.updatedAt: datetime()

///////////////////////////////
// 3. RELATIONSHIPS
///////////////////////////////

// -------- JOB_SKILLS --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/job_skills.csv' AS row
MATCH (j:Job {jobId: row.jobId}),
      (s:Skill {skillId: row.skillId})
MERGE (j)-[r:JOB_SKILLS]->(s)
SET   r.importance = coalesce(row.importance,'Medium'),
      r.mandatory  = (row.mandatory = 'true');


// -------- EMPLOYEE_SKILLS --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/employee_skills.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (s:Skill    {skillId: row.skillId})
MERGE (e)-[r:EMPLOYEE_SKILLS]->(s)
SET   r.proficiency     = coalesce(row.proficiency,'Intermediate'),
      r.yearsExperience = CASE WHEN row.yearsExperience IS NULL OR row.yearsExperience = '' THEN null ELSE toInteger(row.yearsExperience) END;


// -------- JOB_LOCATIONS --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/job_locations.csv' AS row
MATCH (j:Job      {jobId: row.jobId}),
      (l:Location {locationId: row.locationId})
MERGE (j)-[r:JOB_LOCATIONS]->(l)
SET   r.workModel    = coalesce(row.workModel,'Hybrid'),
      r.openPositions = CASE WHEN row.openPositions IS NULL OR row.openPositions = '' THEN null ELSE toInteger(row.openPositions) END;


// -------- EMPLOYEE_LOCATIONS --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/employee_locations.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (l:Location {locationId: row.locationId})
MERGE (e)-[r:EMPLOYEE_LOCATIONS]->(l)
SET   r.primarySite = (coalesce(row.primarySite,'true') = 'true'),
      r.since       = CASE WHEN row.since IS NULL OR row.since = '' THEN null ELSE date(row.since) END;


// -------- EMPLOYEE_JOBS --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/employee_jobs.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (j:Job      {jobId: row.jobId})
MERGE (e)-[r:EMPLOYEE_JOBS]->(j)
SET   r.assignmentType = coalesce(row.assignmentType,'Primary'),
      r.allocation     = CASE WHEN row.allocation IS NULL OR row.allocation = '' THEN null ELSE toInteger(row.allocation) END,
      r.current        = (coalesce(row.current,'true') = 'true'),
      r.assignedAt     = CASE WHEN row.assignedAt IS NULL OR row.assignedAt = '' THEN null ELSE date(row.assignedAt) END;


// -------- REQUIRES_SKILL (Job → Skill) --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/requires_skill.csv' AS row
MATCH (j:Job {jobId: row.jobId}),
      (s:Skill {skillId: row.skillId})
MERGE (j)-[r:REQUIRES_SKILL]->(s)
SET   r.level = coalesce(row.level,'Medium');


// -------- HAS_SKILL (Employee → Skill) --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/has_skill.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (s:Skill {skillId: row.skillId})
MERGE (e)-[r:HAS_SKILL]->(s)
SET   r.proficiency = coalesce(row.proficiency,'Intermediate');


// -------- LOCATED_IN (Employee → Location) --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/located_in.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (l:Location {locationId: row.locationId})
MERGE (e)-[r:LOCATED_IN]->(l)
SET   r.since = CASE WHEN row.since IS NULL OR row.since = '' THEN null ELSE date(row.since) END;


// -------- REPORTS_TO (Employee → Manager) --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/reports_to.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId}),
      (m:Employee {employeeId: row.managerId})
MERGE (e)-[r:REPORTS_TO]->(m)
SET   r.since = CASE WHEN row.since IS NULL OR row.since = '' THEN null ELSE date(row.since) END;


// -------- SKILL_RELATED (Skill → Skill) --------
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/skill_related.csv' AS row
MATCH (s1:Skill {skillId: row.skillId}),
      (s2:Skill {skillId: row.relatedSkillId})
MERGE (s1)-[r:SKILL_RELATED]->(s2)
SET   r.linkStrength = coalesce(row.linkStrength,'Medium');



///////////////////////////////
// 4. VALIDATION QUERIES
///////////////////////////////

MATCH (e:Employee) RETURN 'Employees', count(e);
MATCH (j:Job)      RETURN 'Jobs', count(j);
MATCH (s:Skill)    RETURN 'Skills', count(s);
MATCH (l:Location) RETURN 'Locations', count(l);

MATCH ()-[r:EMPLOYEE_SKILLS]->() RETURN 'EMPLOYEE_SKILLS', count(r);
MATCH ()-[r:JOB_SKILLS]->()      RETURN 'JOB_SKILLS', count(r);
