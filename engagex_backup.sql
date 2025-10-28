--
-- PostgreSQL database dump
--

\restrict SdQNEKmmyZu9egSA6qyJfp3oMCOpb4bBBlKGM3JegEZaei4WPWV32nL05vXUztL

-- Dumped from database version 16.10 (Debian 16.10-1.pgdg13+1)
-- Dumped by pg_dump version 16.10 (Debian 16.10-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: community_ai; Type: SCHEMA; Schema: -; Owner: engagex
--

CREATE SCHEMA community_ai;


ALTER SCHEMA community_ai OWNER TO engagex;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: challenges; Type: TABLE; Schema: community_ai; Owner: engagex
--

CREATE TABLE community_ai.challenges (
    id integer NOT NULL,
    week integer NOT NULL,
    goal text NOT NULL,
    title text NOT NULL,
    description text,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE community_ai.challenges OWNER TO engagex;

--
-- Name: challenges_id_seq; Type: SEQUENCE; Schema: community_ai; Owner: engagex
--

CREATE SEQUENCE community_ai.challenges_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE community_ai.challenges_id_seq OWNER TO engagex;

--
-- Name: challenges_id_seq; Type: SEQUENCE OWNED BY; Schema: community_ai; Owner: engagex
--

ALTER SEQUENCE community_ai.challenges_id_seq OWNED BY community_ai.challenges.id;


--
-- Name: metrics; Type: TABLE; Schema: community_ai; Owner: engagex
--

CREATE TABLE community_ai.metrics (
    id integer NOT NULL,
    challenge_id integer,
    completion_rate double precision,
    engagement_score double precision,
    sales_conversion double precision,
    reactivation_rate double precision,
    analyzed_at timestamp without time zone DEFAULT now()
);


ALTER TABLE community_ai.metrics OWNER TO engagex;

--
-- Name: metrics_id_seq; Type: SEQUENCE; Schema: community_ai; Owner: engagex
--

CREATE SEQUENCE community_ai.metrics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE community_ai.metrics_id_seq OWNER TO engagex;

--
-- Name: metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: community_ai; Owner: engagex
--

ALTER SEQUENCE community_ai.metrics_id_seq OWNED BY community_ai.metrics.id;


--
-- Name: responses; Type: TABLE; Schema: community_ai; Owner: engagex
--

CREATE TABLE community_ai.responses (
    id integer NOT NULL,
    user_id integer,
    challenge_id integer,
    content text,
    created_at timestamp without time zone DEFAULT now()
);


ALTER TABLE community_ai.responses OWNER TO engagex;

--
-- Name: responses_id_seq; Type: SEQUENCE; Schema: community_ai; Owner: engagex
--

CREATE SEQUENCE community_ai.responses_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE community_ai.responses_id_seq OWNER TO engagex;

--
-- Name: responses_id_seq; Type: SEQUENCE OWNED BY; Schema: community_ai; Owner: engagex
--

ALTER SEQUENCE community_ai.responses_id_seq OWNED BY community_ai.responses.id;


--
-- Name: users; Type: TABLE; Schema: community_ai; Owner: engagex
--

CREATE TABLE community_ai.users (
    id integer NOT NULL,
    username text NOT NULL,
    is_active boolean DEFAULT true,
    last_active timestamp without time zone,
    joined_at timestamp without time zone DEFAULT now()
);


ALTER TABLE community_ai.users OWNER TO engagex;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: community_ai; Owner: engagex
--

CREATE SEQUENCE community_ai.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE community_ai.users_id_seq OWNER TO engagex;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: community_ai; Owner: engagex
--

ALTER SEQUENCE community_ai.users_id_seq OWNED BY community_ai.users.id;


--
-- Name: challenges id; Type: DEFAULT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.challenges ALTER COLUMN id SET DEFAULT nextval('community_ai.challenges_id_seq'::regclass);


--
-- Name: metrics id; Type: DEFAULT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.metrics ALTER COLUMN id SET DEFAULT nextval('community_ai.metrics_id_seq'::regclass);


--
-- Name: responses id; Type: DEFAULT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.responses ALTER COLUMN id SET DEFAULT nextval('community_ai.responses_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.users ALTER COLUMN id SET DEFAULT nextval('community_ai.users_id_seq'::regclass);


--
-- Data for Name: challenges; Type: TABLE DATA; Schema: community_ai; Owner: engagex
--

COPY community_ai.challenges (id, week, goal, title, description, created_at) FROM stdin;
1	1	engagement	Поделись мини-лайфхаком продуктивности	Неделя вовлечения	2025-10-21 10:16:47.453143
2	2	retention	Дневная отметка: чем ты продвинулся?	Неделя удержания	2025-10-21 10:16:47.453143
3	3	sales	Попробуй фичу X и напиши отзыв	Неделя конверсий	2025-10-21 10:16:47.453143
4	4	reactivation	Как у тебя дела? Мы скучали	Неделя реактивации	2025-10-21 10:16:47.453143
\.


--
-- Data for Name: metrics; Type: TABLE DATA; Schema: community_ai; Owner: engagex
--

COPY community_ai.metrics (id, challenge_id, completion_rate, engagement_score, sales_conversion, reactivation_rate, analyzed_at) FROM stdin;
\.


--
-- Data for Name: responses; Type: TABLE DATA; Schema: community_ai; Owner: engagex
--

COPY community_ai.responses (id, user_id, challenge_id, content, created_at) FROM stdin;
1	1	1	Планирую день по правилу 1-3-5	2025-10-21 07:16:47.453143
2	2	1	Таймер Помодоро на 25 минут	2025-10-21 08:16:47.453143
3	3	2	Закрыл 2 задачи по проекту	2025-10-20 10:16:47.453143
4	5	3	Фича X ускорила отчёты на 30%	2025-10-21 09:46:47.453143
5	1	4	Вернулся после перерыва 👍	2025-10-21 10:06:47.453143
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: community_ai; Owner: engagex
--

COPY community_ai.users (id, username, is_active, last_active, joined_at) FROM stdin;
1	alice	t	2025-10-20 10:16:47.453143	2025-10-21 10:16:47.453143
2	bob	t	2025-10-21 08:16:47.453143	2025-10-21 10:16:47.453143
3	carol	t	2025-10-18 10:16:47.453143	2025-10-21 10:16:47.453143
4	dave	f	2025-10-01 10:16:47.453143	2025-10-21 10:16:47.453143
5	eric	t	2025-10-21 04:16:47.453143	2025-10-21 10:16:47.453143
\.


--
-- Name: challenges_id_seq; Type: SEQUENCE SET; Schema: community_ai; Owner: engagex
--

SELECT pg_catalog.setval('community_ai.challenges_id_seq', 4, true);


--
-- Name: metrics_id_seq; Type: SEQUENCE SET; Schema: community_ai; Owner: engagex
--

SELECT pg_catalog.setval('community_ai.metrics_id_seq', 1, false);


--
-- Name: responses_id_seq; Type: SEQUENCE SET; Schema: community_ai; Owner: engagex
--

SELECT pg_catalog.setval('community_ai.responses_id_seq', 5, true);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: community_ai; Owner: engagex
--

SELECT pg_catalog.setval('community_ai.users_id_seq', 5, true);


--
-- Name: challenges challenges_pkey; Type: CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.challenges
    ADD CONSTRAINT challenges_pkey PRIMARY KEY (id);


--
-- Name: metrics metrics_pkey; Type: CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.metrics
    ADD CONSTRAINT metrics_pkey PRIMARY KEY (id);


--
-- Name: responses responses_pkey; Type: CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.responses
    ADD CONSTRAINT responses_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: metrics metrics_challenge_id_fkey; Type: FK CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.metrics
    ADD CONSTRAINT metrics_challenge_id_fkey FOREIGN KEY (challenge_id) REFERENCES community_ai.challenges(id);


--
-- Name: responses responses_challenge_id_fkey; Type: FK CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.responses
    ADD CONSTRAINT responses_challenge_id_fkey FOREIGN KEY (challenge_id) REFERENCES community_ai.challenges(id);


--
-- Name: responses responses_user_id_fkey; Type: FK CONSTRAINT; Schema: community_ai; Owner: engagex
--

ALTER TABLE ONLY community_ai.responses
    ADD CONSTRAINT responses_user_id_fkey FOREIGN KEY (user_id) REFERENCES community_ai.users(id);


--
-- PostgreSQL database dump complete
--

\unrestrict SdQNEKmmyZu9egSA6qyJfp3oMCOpb4bBBlKGM3JegEZaei4WPWV32nL05vXUztL

