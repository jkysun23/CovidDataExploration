SELECT * FROM PortfolioProject.dbo.CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3, 4

--SELECT * 
--FROM PortfolioProject.dbo.CovidVaccinations
--ORDER BY 3, 4

SELECT location, date, total_cases, new_cases, total_deaths, population 
FROM PortfolioProject.dbo.CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2


-- Looking at Total Cases vs Total Deaths
-- Shows likelihood of death due to COVID in your country
SELECT location, date, total_cases, total_deaths, ROUND((total_deaths/total_cases)*100, 2) AS DeathPercentage
FROM PortfolioProject.dbo.CovidDeaths
WHERE location like '%states%' AND continent IS NOT NULL
ORDER BY 1, 2

-- Look at Total Cases vs Population
-- Shows percentage of populations who got COVID
SELECT location, date, 
population, total_cases, (total_cases/population)*100 AS PercentagePopulationInfected
FROM PortfolioProject.dbo.CovidDeaths
WHERE location like '%states%' 
ORDER BY 1, 2

-- Look at Countries with Highest Infection Rate compared to Population
SELECT location, 
population, MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS PercentagePopulationInfected
FROM PortfolioProject.dbo.CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY PercentagePopulationInfected DESC


-- Look at Countries with Highest Death Count per Population

SELECT location, MAX(CAST(total_deaths AS int)) as TotalDeathCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC


-- Break things down by continent.  the CORRECT way for visual drilldown
SELECT location, MAX(CAST(total_deaths AS int)) as TotalDeathCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NULL and location NOT LIKE '%income%'
AND location NOT LIKE 'World' AND location NOT LIKE 'International'
AND location NOT LIKE 'European Union'
GROUP BY location
ORDER BY TotalDeathCount DESC
-- World = Europe + North America + Asia + South America + Africa + Oceania + International

-- global numbers  
-- World = Europe + North America + Asia + South America + Africa + Oceania + International
SELECT SUM(new_cases) as total_cases, SUM(CAST(new_deaths as int)) as total_deaths 
, SUM(CAST(new_deaths as int))/SUM(new_cases) * 100 as DeathPercentage from PortfolioProject..CovidDeaths
WHERE continent IS NULL AND new_cases != 0 AND new_deaths != 0
and location NOT LIKE '%income%'
AND location NOT LIKE 'World' AND location NOT LIKE 'International'
AND location NOT LIKE 'European Union'
-- GROUP BY date
ORDER BY 1, 2


-- total_deaths per continent as of 2/15/2022
SELECT location, total_deaths FROM(
SELECT continent, location, total_deaths, ROW_NUMBER() OVER (PARTITION BY location ORDER BY DATE DESC) AS row_num 
FROM PortfolioProject..CovidDeaths
WHERE continent IS NULL
AND location NOT LIKE '%income%'
AND location NOT LIKE 'World' AND location NOT LIKE 'International'
AND location NOT LIKE 'European Union'
) AS partdeaths
WHERE row_num = 1

--total deaths per country as of 2/15/2022
SELECT location, total_deaths FROM(
SELECT continent, location, CAST(total_deaths AS INT) as total_deaths, ROW_NUMBER() OVER (PARTITION BY location ORDER BY DATE DESC) AS row_num 
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
AND total_deaths IS NOT NULL
AND location NOT IN ('%income%', 'World', 'International', 'European Union')
AND location NOT LIKE 'World' AND location NOT LIKE 'International'
AND location NOT LIKE 'European Union'
) AS partdeaths
WHERE row_num = 1
ORDER BY total_deaths DESC



--creating CTE for countries
--CTEs are used to reduce chunkiness of the current query scope.  It makes recursive queries mroe readable.
--temp table is physical storage (in memory and on disk).  Thus temp tables can be referenced multiple times.
-- a subquery can only be referenced within it's query

WITH
cte1 AS(
SELECT continent, location, CAST(total_deaths as int) deaths, date
FROM PortfolioProject..CovidDeaths)
,
cte2 AS(
SELECT continent, location, deaths, DENSE_RANK() OVER (PARTITION BY location ORDER BY deaths DESC) deathranks FROM cte1
WHERE location NOT IN ('%income%', 'World', 'International', 'European Union'))


SELECT * from cte2
WHERE deathranks = 1
AND continent IS NOT NULL
AND deaths IS NOT NULL
ORDER BY deaths DESC

-- ranking total_deaths amongst countries.
WITH cte3 AS (
SELECT continent, location, total_deaths FROM(
SELECT continent, location, total_deaths, ROW_NUMBER() OVER (PARTITION BY location ORDER BY DATE DESC) AS row_num 
FROM PortfolioProject..CovidDeaths
--AND location NOT LIKE 'World' AND location NOT LIKE 'International'
--AND location NOT LIKE 'European Union'
) AS partdeaths
WHERE row_num = 1
--ORDER BY CAST(total_deaths as INT) DESC
)
SELECT location, total_deaths, DENSE_RANK() OVER (ORDER BY CAST(total_deaths as INT) DESC) deaths_ranked FROM cte3
WHERE continent IS NOT NULL
AND total_deaths IS NOT NULL
AND location NOT IN ('%income%', 'World', 'International', 'European Union')

-- Check data types of columns
SELECT COLUMN_NAME, 
	DATA_TYPE, 
    IS_NULLABLE, 
    CHARACTER_MAXIMUM_LENGTH, 
    NUMERIC_PRECISION
FROM PortfolioProject.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='CovidDeaths'


-- Moving on to the Covid Vaccinations table
SELECT * FROM PortfolioProject..CovidVaccinations


-- Check data types
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
FROM PortfolioProject.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CovidVaccinations'

-- Join the two tables together
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations 
, SUM(CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3 


-- Using CTEs
-- CTEs are mostly used to make 
WITH PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
-- you usually specify a list of comma separated columns for JOINS in the CTE
-- make sure the columns match the columns in the CTE definition
-- it's just for clarification, you can set up CTE without specifying column names and it should still work
-- the column list after cte declaration can be used to declare alias for derived columns within the wrapped query.  SEE RollingPeopleVaccinated
-- use the explicitly declared column names instead of the column names in the wrapped query
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations 
, SUM(CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) 
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
)
SELECT *, (RollingPeopleVaccinated/population)*100 AS VaxPercentage FROM PopvsVac
WHERE new_vaccinations IS NOT NULL


-- Using Temp Tables
DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
continent nvarchar(100),
location nvarchar(100),
date datetime,
population float,
new_vaccinations bigint,
RollingPeopleVaccinated bigint,
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations 
, SUM(CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL


SELECT *, (RollingPeopleVaccinated/population)*100 AS VaxPercentage
FROM #PercentPopulationVaccinated
WHERE new_vaccinations IS NOT NULL


-- Creating View to store for later visualization
USE PortfolioProject
GO
CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations 
, SUM(CAST(vac.new_vaccinations AS bigint)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL


SELECT * FROM PortfolioProject..PercentPopulationVaccinated

