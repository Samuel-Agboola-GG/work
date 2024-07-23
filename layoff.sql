-- Data cleaning
-- Remove Duplicates
-- standardize the data
-- Remove Null and Blanks
-- Remove any columns

SELECT * 
FROM layoffs;



CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Removing duplicate : by adding a row number over the rows partioned by using columns resulting created column will indicates duplicated rows
SELECT *,
ROW_NUMBER() 
OVER
(PARTITION BY company, industry, percentage_laid_off, total_laid_off, `date`) AS row_num
FROM layoffs_staging;
-- create CTE to see the duplicated data
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() 
OVER
(PARTITION BY company, industry, percentage_laid_off, total_laid_off, `date`, country, stage, funds_raised_millions, location) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- to delete the duplicate, create new table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER
(PARTITION BY company, industry, percentage_laid_off, total_laid_off, `date`, country, stage, funds_raised_millions, location) AS row_num
FROM layoffs_staging;
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- standardization
-- trimming spaces

SELECT * 
FROM layoffs_staging2;
SELECT TRIM(company),company
FROM layoffs_staging2;
-- update trimmed column
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- crypto needs to have same label

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States'
;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';
-- converting date from text to date_formate. convert data in the column before converting data type to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
 SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;
