select *
from layoffs;

-- -- first thing we want to do is create a staging table. This is the one we will work in and clean the data.
-- We want a table with the raw data in case something happens
CREATE TABLE layoff_stag
LIKE layoffs;

select *
from layoff_stag;

INSERT layoff_stag
SELECT *
FROM layoffs;

-- Removing duplicate values

WITH duplicate_cte AS
(
  Select * ,
  ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
  FROM layoff_stag
  )
  select *
  FROM duplicate_cte
  WHERE row_num > 1;
  
-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
-- now you may want to write it like this:

WITH duplicate_cte AS
(
  Select * ,
  ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
  FROM layoff_stag
  )
  delete 
  FROM duplicate_cte
  WHERE row_num > 1;
  
  -- one solution, Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- create a new table similar to layoff_stag and insert the data 

  CREATE TABLE `layoff_stag2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` Int
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_stag2
Select * ,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoff_stag;

select * 
from layoff_stag2
where row_num > 1;

-- now that we have row_num greater than 2 deletion can be performed
delete 
from layoff_stag2
where row_num > 1;

-- standardize data
-- whitespace and full stop can be removed from data as shown below 
select company, trim(company)
from layoff_stag2;

update layoff_stag2
set company = trim(company);

select Distinct industry
from layoff_stag2;

-- As noticed there were industry named with variety of crypto name so updated accordingly 
update layoff_stag2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoff_stag2
order by 1;

-- noticed at few places a variety of same country name with variation 
Update layoff_stag2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- updating date from text type to correct data type
update layoff_stag2
set `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoff_stag2
modify column `date` DATE;

-- working on nulls

select *
from layoff_stag2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoff_stag2
where industry is null
or industry = '';

select l1.industry,l2.industry
from layoff_stag2 l1
join layoff_stag2 l2
  on l1.company =l2.company
where (l1.industry is null or l1.industry = '')
and l2.industry is not null;

-- it looks like airbnb is a travel, but this one just isn't populated.
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with

select *
from layoff_stag2
where company = 'Airbnb';

update layoff_stag2
set industry = null
where industry ='';

-- now the null can be replaced with a value based on the query
update layoff_stag2 t1
join layoff_stag2 t2
  on t1.company = t2.company
set t1.industry =t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoff_stag2
where total_laid_off is null
and percentage_laid_off is null;

-- deleting the rows we can't use it
delete
from layoff_stag2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoff_stag2;

-- already deleted the duplicate rows so we can delete the extra column which will not be used on the later stage
ALTER TABLE layoff_stag2
DROP COLUMN row_num;

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal.
-- Great ! Now we have a cleaned dataset we can work for EDA









  
  