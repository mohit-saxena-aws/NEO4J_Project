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
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/locations.csv' AS row
MERGE (loc:Location {locationId: row.locationId})
SET  loc.locationId = row.locationId,
     loc.officeName = row.officeName,
     loc.city       = row.city,
     loc.state      = row.state,
     loc.country    = row.country,
     loc.region     = row.region,
     loc.timezone   = row.timezone,
     loc.latitude   = CASE WHEN row.latitude IS NULL  OR row.latitude = '' THEN null ELSE toFloat(row.latitude) END,
     loc.longitude  = CASE WHEN row.longitude IS NULL OR row.longitude = '' THEN null ELSE toFloat(row.longitude) END,
     loc.address: row.address,
     loc.postalCode: row.postalCode,
     loc.locationType: row.locationType,
     loc.capacity: row.capacity,
     loc.siteManager: row.siteManager,
     loc.createdAt: datetime()

// -------- Skills --------
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/skills.csv' AS row
MERGE (s:Skill {skillId: row.skillId})
SET  s.skillId: row.skillId,
     s.name: row.name,
     s.slug: row.slug,
     s.category: row.category,
     s.subCategory: row.subCategory,
     s.description: coalesce(row.description,''),
     s.proficiencyScale: row.proficiencyScale,
     s.commonCertifications: row.commonCertifications,
     s.lastUpdated: date(row.lastUpdated),
     s.popularityRank: row.popularityRank,
     s.industryStandard: row.industryStandard,
     s.createdAt: datetime()

// -------- Jobs --------
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/jobs.csv' AS row
MERGE (j:Job {jobId: row.jobId})
SET   j.jobId: row.jobId,
      j.title: row.title,
      j.department: coalesce(row.department,''),
      j.grade: row.grade,
      j.seniority: row.seniority,
      j.roleType: row.roleType,
      j.employmentBand: row.employmentBand,
      j.minSalary  = CASE WHEN row.minSalary IS NULL OR row.minSalary = '' THEN null ELSE toInteger(row.minSalary) END,
      j.maxSalary  = CASE WHEN row.maxSalary IS NULL OR row.maxSalary = '' THEN null ELSE toInteger(row.maxSalary)
      j.currency: row.currency,
      j.importance = coalesce(row.importance,'Medium'),
      j.mandatory  = (row.mandatory = 'true');
      j.remoteFriendly: row.remoteFriendly,
      j.responsibilities: row.responsibilities,
      j.requiredExperienceYears: row.requiredExperienceYears,
      j.jobDescription: row.jobDescription,
      j.requisitionStatus: row.requisitionStatus,
      j.requisitionCreatedAt: date(row.requisitionCreatedAt),
      j.createdAt: datetime()

// -------- Employees (LARGE FILE) --------

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
// 3. RELATIONSHIP PATTERNS (enterprise-grade)
///////////////////////////////

// ===================================================
// I. Relationship: (:Employee)-[:WORKS_AS]->(:Job)
// ===================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_jobs.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (j:Job {JobID: row.JobID})
MERGE (e)-[r:WORKS_AS]->(j)
SET r.startDate = date(row.startDate),
    r.endDate = date(row.endDate),
    r.contractType = row.contractType,
    r.assignmentType = row.assignmentType,
    r.primaryAssignment = row.primaryAssignment,
    r.allocationPercent = row.allocationPercent,
    r.current        = (coalesce(row.current,'true') = 'true'),
    r.assignedAt     = CASE WHEN row.assignedAt IS NULL OR row.assignedAt = '' THEN null ELSE date(row.assignedAt) END;
    r.roleContext = row.roleContext,
    r.jobLevel = row.jobLevel,
    r.updatedAt = datetime();

// =======================================================
// II. Relationship: (:Employee)-[:LOCATED_IN]->(:Location)
// =======================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_locations.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (l:Location {LocationID: row.LocationID})
MERGE (e)-[:LOCATED_IN]->(l);
SET r.since = CASE WHEN row.since IS NULL OR row.since = '' THEN null ELSE date(row.since) END,
    r.workPattern = row.workPattern,
    r.floor = row.floor,
    r.deskNo = row.deskNo,
    r.badgeAccess = row.badgeAccess,
    r.remoteEligibility = row.remoteEligibility;
    r.primarySite = row.primarySite,
    r.updatedAt = datetime();

// ====================================================
// III. Relationship: (:Employee)-[:HAS_SKILL]->(:Skill)
// ====================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/employee_skills.csv' AS row
MATCH (e:Employee {EmployeeID: row.EmployeeID})
MATCH (s:Skill {SkillID: row.SkillID})
MERGE (e)-[:HAS_SKILL]->(s);
SET r.proficiency = coalesce(row.proficiency,'Intermediate'),
    r.yearsExperience = row.yearsExperience,
    r.lastUsed = date(row.lastUsed),
    r.certified = row.certified,
    r.certificationName = row.certificationName,
    r.verified = row.verified,
    r.endorsements = row.endorsements,
    r.confidenceScore = row.confidenceScore,
    r.recordedAt = row.recordedAt,
    r.updatedAt = datetime();

// ==================================================
// IV. Relationship: (:Job)-[:REQUIRES_SKILL]->(:Skill)
// ==================================================
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/requires_skill.csv' AS row
MATCH (j:Job {JobID: row.JobID})
MATCH (s:Skill {SkillID: row.SkillID})
MERGE (j)-[:REQUIRES_SKILL]->(s);
SET r.importance = "High",
    r.mandatory = true,
    r.minExperience = 5,
    r.certPreferred = true,
    r.priorityRank = 1,
    r.level = coalesce(row.level,'Medium'),
    r.updatedAt = datetime();

// ==================================================
// V. Relationship: (:Job)-[:BASED_IN]->(:Locations)
// ==================================================
/* JOB -> BASED_IN -> LOCATION (canonical) */
USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM
'https://YOUR_BUCKET/job_locations.csv' AS row
MATCH (j:Job      {jobId: row.jobId}),
      (l:Location {locationId: row.locationId})
MERGE (j)-[r:JOB_LOCATIONS]->(l)
SET   r.workModel    = coalesce(row.workModel,'Hybrid'),
      r.onsiteRequired = row.onsiteRequired,
      r.shiftWindow = row.shiftWindow,
      r.updatedAt = datetime(),
      r.hoursOverlap = row.hoursOverlap,
      r.visaSupport = row.visaSupport,
      r.regionAlignment = row.regionAlignment,
      r.openPositions = CASE WHEN row.openPositions IS NULL OR row.openPositions = '' THEN null ELSE toInteger(row.openPositions) END;


// ==================================================
// VI. Relationship: (:Skills)-[:SKILL_RELATED]->(:Skills)
// ==================================================
/* SKILL -> SKILL_RELATED -> SKILL */
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/skill_related.csv' AS row
WITH row, row.SkillID AS A, row.RelatedSkillID AS S2
WHERE S1 < S2
MATCH (s1:Skill {SkillID: S1})
MATCH (s2:Skill {SkillID: S2})
MERGE (s1)-[:RELATED_TO]->(s2);
SET r.relationType = row.relationType,
    r.linkStrength = coalesce(row.linkStrength,'Medium'),
    r.updatedAt = datetime();


// =======================================================
// VII. Relationship: (:Employee)-[:REPORTS_TO]->(:Employee)
// =======================================================
// -------- REPORTS_TO (Employee → Manager) --------
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Import/reports_to.csv' AS row
MATCH (e1:Employee {EmployeeID: row.EmployeeID})
MATCH (e2:Employee {EmployeeID: row.ManagerID})
MERGE (e1)-[:REPORTS_TO]->(e2);
SET r.since = CASE WHEN row.since IS NULL OR row.since = '' THEN null ELSE date(row.since) END,
    r.relationshipType = row.relationshipType,
    r.updatedAt = datetime();


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
