///////////////////////////////////////////////////////////////////////////
// AURA-READY MASTER LOADER FOR COMPLETE GRAPH DB
// Replace the placeholder URLs with your S3/ GitHub / GCS / HTTPS CSV links.
// This script is idempotent and safe to re-run.
// Optimized for Aura using LOAD CSV + PERIODIC COMMIT.
///////////////////////////////////////////////////////////////////////////


///////////////////////////////
// 1. UNIQUE CONSTRAINTS & INDEXES
///////////////////////////////

CREATE CONSTRAINT employee_id IF NOT EXISTS
FOR (e:Employee)
REQUIRE e.employeeId IS UNIQUE;

CREATE CONSTRAINT job_id IF NOT EXISTS
FOR (j:Job)
REQUIRE j.jobId IS UNIQUE;

CREATE CONSTRAINT location_id IF NOT EXISTS
FOR (l:Location)
REQUIRE l.locationId IS UNIQUE;

CREATE CONSTRAINT skill_id IF NOT EXISTS
FOR (s:Skill)
REQUIRE s.skillId IS UNIQUE;

CREATE INDEX employee_lastName IF NOT EXISTS
FOR (e:Employee)
ON (e.lastName);

CREATE INDEX employee_email IF NOT EXISTS
FOR (e:Employee)
ON (e.email);

CREATE INDEX job_title IF NOT EXISTS
FOR (j:Job)
ON (j.title);

CREATE INDEX location_city IF NOT EXISTS
FOR (l:Location)
ON (l.city);

CREATE INDEX skill_name IF NOT EXISTS
FOR (s:Skill)
ON (s.name);


///////////////////////////////
// 2. LOAD NODE FILES
///////////////////////////////


// -------- Locations --------
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/locations.csv' AS row
MERGE (l:Location {locationId: row.locationId})
SET  loc.locationId = row_loc.locationId,
     loc.officeName = row_loc.officeName,
     loc.city       = row_loc.city,
     loc.state      = row_loc.state,
     loc.country    = row_loc.country,
     loc.region     = row_loc.region,
     loc.timezone   = row_loc.timezone,
     loc.latitude   = CASE WHEN row_loc.latitude IS NULL  OR row_loc.latitude = '' THEN null ELSE toFloat(row_loc.latitude) END,
     loc.longitude  = CASE WHEN row_loc.longitude IS NULL OR row_loc.longitude = '' THEN null ELSE toFloat(row_loc.longitude) END,
     loc.address    = row_loc.address,
     loc.postalCode = row_loc.postalCode,
     loc.locationType = row_loc.locationType,
     loc.capacity = row_loc.capacity,
     loc.siteManager  = row_loc.siteManager,
     loc.createdAt = datetime()

// -------- Skills --------
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/skills.csv' AS row
MERGE (s:Skill {skillId: row.skillId})
SET  s.skillId  = row_skill.skillId,
     s.name  = row_skill.name,
     s.slug  = row_skill.slug,
     s.category  = row_skill.category,
     s.subCategory  = row_skill.subCategory,
     s.description  = coalesce(row_skill.description,''),
     s.proficiencyScale  = row_skill.proficiencyScale,
     s.commonCertifications  = row_skill.commonCertifications,
     s.lastUpdated  = date(row_skill.lastUpdated),
     s.popularityRank  = row_skill.popularityRank,
     s.industryStandard  = row_skill.industryStandard,
     s.createdAt  = datetime()
WITH *
RETURN count(s);

// -------- Jobs --------
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/jobs.csv' AS row
MERGE (j:Job {jobId: row.jobId})
SET   j.jobId  = row_job.jobId,
      j.title  = row_job.title,
      j.department  = coalesce(row_job.department,''),
      j.grade  = row_job.grade,
      j.seniority  = row_job.seniority,
      j.roleType  = row_job.roleType,
      j.employmentBand  = row_job.employmentBand,
      j.minSalary  = CASE WHEN row_job.minSalary IS NULL OR row_job.minSalary = '' THEN null ELSE toInteger(row_job.minSalary) END,
      j.maxSalary  = CASE WHEN row_job.maxSalary IS NULL OR row_job.maxSalary = '' THEN null ELSE toInteger(row_job.maxSalary) END,
      j.currency  = row_job.currency,
      j.importance = coalesce(row_job.importance,'Medium'),
      j.mandatory  = (row_job.mandatory = 'true');
      j.remoteFriendly  = row_job.remoteFriendly,
      j.responsibilities  = row_job.responsibilities,
      j.requiredExperienceYears  = row_job.requiredExperienceYears,
      j.jobDescription  = row_job.jobDescription,
      j.requisitionStatus  = row_job.requisitionStatus,
      j.requisitionCreatedAt  = date(row_job.requisitionCreatedAt),
      j.createdAt  = datetime()
WITH *
RETURN count(s);

// -------- Employees (LARGE FILE) --------

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/employees.csv' AS row
MERGE (e:Employee {employeeId: row.employeeId})
SET  e.employeeId  = row_emp.employeeId,
     e.firstName  = row_emp.firstName,
     e.lastName  = row_emp.lastName,
     e.displayName  = row_emp.displayName,
     e.preferredName  = row_emp.preferredName,
     e.email  = row_emp.email,
     e.workPhone  = row_emp.workPhone,
     e.personalPhone  = row_emp.personalPhone,
     e.hireDate  = date(row_emp.hireDate),
     e.birthDate  = date(row_emp.birthDate),
     e.title  = coalesce(row_emp.title),
     e.jobLevel  = coalesce(row_emp.jobLevel),
     e.employmentType  = row_emp.employmentType,
     e.status  = row_emp.status,
     e.experienceYears  = row_emp.experienceYears,
     e.salaryAnnual  = row_emp.salaryAnnual,
     e.salaryCurrency  = row_emp.salaryCurrency,
     e.nationality  = row_emp.nationality,
     e.locationId  = coalesce(row_emp.locationId,''),
     e.languages  = row_emp.languages,
     e.workModel  = row_emp.workModel,
     e.linkedin  = row_emp.linkedin,
     e.managerEmployeeId  = row_emp.managerEmployeeId,
     e.hrEmployeeNumber  = row_emp.hrEmployeeNumber,
     e.officeBadge  = row_emp.officeBadge,
     e.createdAt  = datetime(),
     e.updatedAt  = datetime()
WITH *
RETURN count(s);


///////////////////////////////
// 3. RELATIONSHIP PATTERNS (enterprise-grade)
///////////////////////////////

// ===================================================
// I. Relationship: (:Employee)-[:WORKS_AS]->(:Job)
// ===================================================

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/employee_jobs.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId})
MATCH (j:Job {jobId: row.jobId})
MERGE (e)-[:WORKS_AS]->(j)
RETURN count(*);



// =======================================================
// II. Relationship: (:Employee)-[:LOCATED_IN]->(:Location)
// =======================================================

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/employee_locations.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId})
MATCH (l:Location {locationId: row.locationId})
MERGE (e)-[:LOCATED_IN]->(l)
RETURN count(*);

// ====================================================
// III. Relationship: (:Employee)-[:HAS_SKILL]->(:Skill)
// ====================================================

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/employee_skills.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId})
MATCH (s:Skill {skillId: row.skillId})
MERGE (e)-[:HAS_SKILL {proficiency: row.proficiency}]->(s)
RETURN count(*);

// ==================================================
// IV. Relationship: (:Job)-[:REQUIRES_SKILL]->(:Skill)
// ==================================================

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/requires_skill.csv' AS row
MATCH (j:Job {jobId: row.jobId})
MATCH (s:Skill {skillId: row.skillId})
MERGE (j)-[:REQUIRES_SKILL]->(s)
RETURN count(*);

// ==================================================
// V. Relationship: (:Job)-[:BASED_IN]->(:Locations)
// ==================================================
/* JOB -> BASED_IN -> LOCATION (canonical) */

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/job_locations.csv' AS row
MATCH (j:Job {jobId: row.jobId})
MATCH (l:Location {locationId: row.locationId})
MERGE (j)-[:BASED_IN]->(l)
RETURN count(*);

// ==================================================
// VI. Relationship: (:Skills)-[:SKILL_RELATED]->(:Skills)
// ==================================================
/* SKILL -> SKILL_RELATED -> SKILL */

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/skill_related.csv' AS row
MATCH (a:Skill {skillId: row.skillId1})
MATCH (b:Skill {skillId: row.skillId2})
WITH a, b
WHERE a.skillId < b.skillId    // ensure unique undirected pair
MERGE (a)-[:RELATED_TO]-(b)
RETURN count(*);

// =======================================================
// VII. Relationship: (:Employee)-[:REPORTS_TO]->(:Employee)
// =======================================================
// -------- REPORTS_TO (Employee → Manager) --------

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mohit-saxena-aws/NEO4J_Project/main/Import/reports_to.csv' AS row
MATCH (e:Employee {employeeId: row.employeeId})
MATCH (m:Employee {employeeId: row.managerId})
MERGE (e)-[:REPORTS_TO]->(m)
RETURN count(*);

// ======================
// VIII. Completion Message
// ======================
RETURN "AURA LOADER COMPLETED SUCCESSFULLY";

///////////////////////////////
// 4. VALIDATION QUERIES
///////////////////////////////

MATCH (e:Employee) RETURN 'Employees', count(e);
MATCH (j:Job)      RETURN 'Jobs', count(j);
MATCH (s:Skill)    RETURN 'Skills', count(s);
MATCH (l:Location) RETURN 'Locations', count(l);

MATCH ()-[r:EMPLOYEE_SKILLS]->() RETURN 'EMPLOYEE_SKILLS', count(r);
MATCH ()-[r:JOB_SKILLS]->()      RETURN 'JOB_SKILLS', count(r);


// =====================================================
// 5) EXAMPLE CARDINALITY PATTERNS (as comments & queries)
// =====================================================

/*
Cardinality guidance (examples):
 - One Job can require many Skills: (Job)-[:JOB_SKILLS]->(Skill)  (1..N)
 - One Skill can be required by many Jobs: (Job)-[:JOB_SKILLS]->(Skill)  (N..M)
 - One Employee can have many Skills: (Employee)-[:EMPLOYEE_SKILLS]->(Skill) (0..N)
 - One Employee can have multiple Job assignments: (Employee)-[:EMPLOYEE_JOBS]->(Job) (0..N)
 - One Job can be available in multiple Locations: (Job)-[:JOB_LOCATIONS]->(Location) (1..N)
 - One Location can host many Employees: (Location)<-[:EMPLOYEE_LOCATIONS]-(Employee) (1..N)
*/

/* Query to count skills per job */
MATCH (j:Job)-[r:JOB_SKILLS]->(s:Skill)
RETURN j.jobId AS jobId, j.title AS jobTitle, count(s) AS requiredSkillCount
ORDER BY requiredSkillCount DESC;

/* Query to find employees with primary skill matching a job requirement */
MATCH (j:Job {jobId:"J2001"})-[:JOB_SKILLS]->(req:Skill)
MATCH (e:Employee)-[es:EMPLOYEE_SKILLS]->(req)
WHERE es.primarySkill = true OR es.proficiency IN ["Expert","Advanced"]
RETURN e.employeeId, e.displayName, collect(req.name) AS matchedSkills, sum(COALESCE(es.yearsExperience,0)) AS totalSkillYears
ORDER BY totalSkillYears DESC;
