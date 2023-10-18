CREATE DATABASE PortfolioProject;
USE PortfolioProject;
SHOW VARIABLES LIKE "secure_file_priv";
DROP TABLE CovidDeaths;
DROP TABLE CovidVaccinations;

-- Preparing data

-- Making table CovidDeaths
CREATE TABLE CovidDeaths
(
iso_code VARCHAR(50), 
continent VARCHAR(50), 
location VARCHAR(50), 
date date, 
population bigint, 
total_cases int, 
new_cases int,
new_cases_smoothed float, 
total_deaths int,
new_deaths int,
new_deaths_smoothed float, 
total_cases_per_million float, 
new_cases_per_million float,
new_cases_smoothed_per_million float, 
total_deaths_per_million float,
new_deaths_per_million float,
new_deaths_smoothed_per_million float, 
reproduction_rate float,
icu_patients INT,
icu_patients_per_million float, 
hosp_patients INT,
hosp_patients_per_million float, 
weekly_icu_admissions float,
weekly_icu_admissions_per_million float,
weekly_hosp_admissions float,
weekly_hosp_admissions_per_million float
) Engine = InnoDB;

-- Importing data from csv file
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.1\\Uploads\\CovidDeaths.csv' INTO TABLE CovidDeaths
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT * FROM CovidDeaths;

-- Change null from data null
SELECT * FROM CovidDeaths WHERE continent = 'null';
UPDATE CovidDeaths SET continent = null WHERE continent = 'null';
SELECT * FROM CovidDeaths WHERE continent is null;


-- Making table CovidVaccinations
CREATE TABLE CovidVaccinations
(
iso_code VARCHAR(50),
continent VARCHAR(50),
location VARCHAR(50),
date DATE,
new_tests INT,
total_tests INT, 
total_tests_per_thousand  float,
new_tests_per_thousand float,
new_tests_smoothed INT,
new_tests_smoothed_per_thousand float, 
positive_rate float,
tests_per_case float, 
tests_units VARCHAR(100),
total_vaccinations BIGINT, 
people_vaccinated BIGINT,
people_fully_vaccinated BIGINT, 
new_vaccinations BIGINT,
new_vaccinations_smoothed BIGINT, 
total_vaccinations_per_hundred FLOAT, 
people_vaccinated_per_hundred FLOAT,
people_fully_vaccinated_per_hundred FLOAT, 
new_vaccinations_smoothed_per_million INT,
stringency_index FLOAT,
population_density FLOAT,
median_age FLOAT,
aged_65_older FLOAT,
aged_70_older FLOAT,
gdp_per_capita FLOAT,
extreme_poverty FLOAT,
cardiovasc_death_rate FLOAT,
diabetes_prevalence FLOAT,
female_smokers FLOAT,
male_smokers FLOAT,
handwashing_facilities FLOAT, 
hospital_beds_per_thousand FLOAT, 
life_expectancy FLOAT,
human_development_index FLOAT
) Engine = InnoDB;

-- Importing data from csv file
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.1\\Uploads\\CovidVaccinations.csv' INTO TABLE CovidVaccinations
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT * FROM CovidVaccinations;

-- -- Change null from data null
SELECT * FROM CovidVaccinations WHERE continent = 'null';
UPDATE CovidVaccinations SET continent = null WHERE continent = 'null';
SELECT * FROM CovidVaccinations WHERE continent is null;
SELECT * FROM CovidVaccinations WHERE tests_units = 'null';
UPDATE CovidVaccinations SET tests_units = null WHERE tests_units = 'null';
SELECT * FROM CovidVaccinations WHERE tests_units is null;


-- Data Eksploration
SELECT * FROM PortfolioProject.CovidDeaths ORDER BY location, date;
SELECT * FROM PortfolioProject.CovidVaccinations ORDER BY location, date;

-- Select Data that we are going to be using
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProject.CovidDeaths 
WHERE continent is not null
ORDER BY location, date;

-- Looking at total cases vs total deaths
-- Shows likelihood of dying if you contract covid in Indonesia
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM PortfolioProject.CovidDeaths 
WHERE location LIKE '%Indonesia%'
ORDER BY location, date;

-- Looking at total cases vs population
-- Show percentage of population got Covid
SELECT location, date, population, total_cases, (total_cases/population)*100 as PercentPopulationInfected
FROM PortfolioProject.CovidDeaths 
-- WHERE location LIKE '%Indonesia%'
WHERE continent is not null
ORDER BY location, date;

-- Looking at Countries with Highest Infection Rate compared to Population
SELECT location, MAX(total_cases) AS HighestInfectionCount, population, MAX((total_cases/population)*100) as PercentPopulationInfected
FROM PortfolioProject.CovidDeaths 
-- WHERE location LIKE '%Indonesia%'
WHERE continent is not null
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC;

-- Showing Countries with Highest Death Count Per Population
SELECT location, MAX(total_deaths) AS TotalDeathCount
FROM PortfolioProject.CovidDeaths 
-- WHERE location LIKE '%Indonesia%'
WHERE continent is not null
GROUP BY location
ORDER BY TotalDeathCount DESC;


-- 	LET'S BREAK THINS DOWN BY CONTINENT

-- Showing the continent with the highest death count per population
SELECT continent, MAX(total_deaths) AS TotalDeathCount
FROM PortfolioProject.CovidDeaths 
-- WHERE location LIKE '%Indonesia%'
WHERE continent is not null
GROUP BY continent
ORDER BY TotalDeathCount DESC;


-- GLOBAL NUMBERS
SELECT date, SUM(new_deaths) as total_deaths, SUM(new_cases) as total_cases, (SUM(new_deaths)/SUM(new_cases))*100 as DeathPercentage
FROM PortfolioProject.CovidDeaths 
-- WHERE location LIKE '%Indonesia%'
WHERE continent is not null
GROUP BY date
ORDER BY date;

-- Looking total population vs vaccinations

-- USE CTE
With Popvsvac (Continent, Location, Date, Population, New_Vaccination, RollingPeopleVaccinated)
as (
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
    AS RollingPeopleVaccinated
FROM PortfolioProject.CovidDeaths as dea
JOIN PortfolioProject.CovidVaccinations as vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2,3
) 
SELECT *, (RollingPeopleVaccinated/Population)*100 as PercentPopulationVaccinated
FROM Popvsvac;

-- TEMP TABLE
DROP TABLE PercentPopulationvaccinated;
CREATE TEMPORARY TABLE PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
    AS RollingPeopleVaccinated
FROM PortfolioProject.CovidDeaths as dea
JOIN PortfolioProject.CovidVaccinations as vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2,3;

SELECT * FROM PercentPopulationVaccinated;


-- Creating view to store data for later visualization
CREATE VIEW PercentPopulationVaccinated as
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
    AS RollingPeopleVaccinated
FROM PortfolioProject.CovidDeaths as dea
JOIN PortfolioProject.CovidVaccinations as vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2,3;


SELECT * FROM PercentPopulationVaccinated;






