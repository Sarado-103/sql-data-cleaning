-- Data Cleaning Project SQL

select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values or Blank values
-- 4. Remove any Columns 

CREATE TABLE layoffs_staging
LIKE layoffs;

insert layoffs_staging
select * from layoffs;

								-- 1. Remove Duplicates should not be > than 1

select *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, date) AS row_num
from layoffs_staging;

WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select * from 
duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = "Oda";

WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
date, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
DELETE
FROM duplicate_cte
where row_num > 1;


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2 ;

INSERT INTO layoffs_staging2
Select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
date, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

select *
from layoffs_staging2;

						-- Standardizing Data (Finding issues in data and fixing it.)

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
Where industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(Trailing '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE 'United States%';

select date
from layoffs_staging2;


UPDATE layoffs_staging2
SET date = str_to_date(date, '%m/%d/%Y');

ALTER TABLE 
layoffs_staging2
MODIFY COLUMN date DATE;

select * from layoffs_staging2;
 
											-- NULL and BLANK values

select * from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select DISTINCT industry from layoffs_staging2;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

select * 
from layoffs_staging2
WHERE industry IS NULL 
or industry = '';

select * 
from layoffs_staging2
where company LIKE 'Bally%';

SELECT *
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
and t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
on t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
and t2.industry IS NOT NULL;


SELECT company, location, percentage_laid_off, `date`, stage, country, funds_raised_millions,
       COUNT(*) AS count
FROM layoffs_staging2
GROUP BY company, location, percentage_laid_off, `date`, stage, country, funds_raised_millions
HAVING COUNT(*) > 1;	

select * from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;
											-- 4. Remove any Columns 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
