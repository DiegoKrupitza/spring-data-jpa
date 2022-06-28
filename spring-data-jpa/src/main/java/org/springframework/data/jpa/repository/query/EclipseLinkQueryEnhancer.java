/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jpa.repository.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.eclipse.persistence.jpa.jpql.EclipseLinkGrammarValidator;
import org.eclipse.persistence.jpa.jpql.JPQLQueryProblem;
import org.eclipse.persistence.jpa.jpql.JPQLQueryProblemMessages;
import org.eclipse.persistence.jpa.jpql.parser.*;
import org.springframework.data.domain.Sort;

public class EclipseLinkQueryEnhancer implements QueryEnhancer {

	// https://docs.oracle.com/javaee/6/tutorial/doc/bnbuf.html

	private final DeclaredQuery query;
	private Expression memorizedRootExpression;

	/**
	 * @param query the query we want to enhance. Must not be {@literal null}.
	 */
	public EclipseLinkQueryEnhancer(DeclaredQuery query) {
		this.query = query;
	}

	@Override
	public String applySorting(Sort sort, String alias) {
		// TODO: impl
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public String detectAlias() {
		Expression queryStatement = parseQuery();

		if (!(queryStatement instanceof SelectStatement selectStatement)) {
			// TODO: warning
			throw new IllegalArgumentException("Cannot work with update delete statement");
		}

		FromClause fromClause = (FromClause) selectStatement.getFromClause();

		IdentificationVariableDeclaration declaration;

		if (fromClause.getDeclaration()instanceof IdentificationVariableDeclaration identificationVariableDeclaration) {
			declaration = identificationVariableDeclaration;
		} else if (fromClause.getDeclaration()instanceof CollectionExpression collectionExpression) {
			if (collectionExpression.childrenSize() == 0) {
				throw new IllegalStateException("Collection expression within the form clause cannot be empty!");
			}
			declaration = (IdentificationVariableDeclaration) collectionExpression.children().iterator().next();
		} else {
			throw new IllegalStateException("From clause contains object that we did not expect! %s"
					.formatted(fromClause.getDeclaration().getQueryBNF().toString()));
		}

		RangeVariableDeclaration rangeVariableDeclaration = (RangeVariableDeclaration) declaration
				.getRangeVariableDeclaration();

		return rangeVariableDeclaration.getIdentificationVariable().toActualText();
	}

	@Override
	public String createCountQueryFor(String countProjection) {

		Expression queryStatement = parseQuery();
		SelectStatement selectStatement = (SelectStatement) queryStatement;

		if (!(queryStatement instanceof SelectStatement)) {
			// TODO: warning
			throw new IllegalArgumentException("Cannot work with update delete statement ");
		}

		String alias = detectAlias();

		// TODO: impl

		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public String getProjection() {
		Expression queryStatement = parseQuery();

		if (!(queryStatement instanceof SelectStatement)) {
			// TODO: warning
			throw new IllegalArgumentException("Cannot work with updaet delete statemtn ");
		}

		SelectClause selectClause = (SelectClause) ((SelectStatement) queryStatement).getSelectClause();

		return selectClause.children().toString().replaceFirst("\\[", "").replace("]", "");
	}

	@Override
	public Set<String> getJoinAliases() {
		Set<String> joinAliases = new HashSet<>();

		Expression queryStatement = parseQuery();

		if (!(queryStatement instanceof SelectStatement selectStatement)) {
			// TODO: warning
			throw new IllegalArgumentException("Cannot work with updaet delete statemtn ");
		}

		FromClause fromClause = (FromClause) selectStatement.getFromClause();

		if (fromClause.getDeclaration()instanceof CollectionExpression collectionExpression) {
			joinAliases = StreamSupport.stream(collectionExpression.children().spliterator(), false) //
					.filter(IdentificationVariableDeclaration.class::isInstance) //
					.map(IdentificationVariableDeclaration.class::cast) //
					.flatMap(item -> this.extractJoinsAliases(item).stream())//
					.collect(Collectors.toSet());

		} else if (fromClause
				.getDeclaration()instanceof IdentificationVariableDeclaration identificationVariableDeclaration) {
			joinAliases.addAll(extractJoinsAliases(identificationVariableDeclaration));
		}

		return joinAliases;
	}

	private Collection<String> extractJoinsAliases(IdentificationVariableDeclaration identificationVariableDeclaration) {
		Collection<String> joinAliases = new HashSet<>();

		Expression joins = identificationVariableDeclaration.getJoins();

		if (joins instanceof CollectionExpression collectionExpression) {
			return StreamSupport.stream(collectionExpression.children().spliterator(), false).map(Join.class::cast)
					.map(item -> item.getIdentificationVariable().toActualText()).collect(Collectors.toSet());
		} else if (joins instanceof Join join) {
			return Set.of(join.getIdentificationVariable().toActualText());
		}

		return joinAliases;
	}

	@Override
	public boolean hasConstructorExpression() {

		Expression queryStatement = parseQuery();

		if (!(queryStatement instanceof SelectStatement)) {
			// TODO: warning
			throw new IllegalArgumentException("Cannot work with updaet delete statemtn ");
		}

		SelectClause selectClause = (SelectClause) ((SelectStatement) queryStatement).getSelectClause();
		Expression selectExpression = selectClause.getSelectExpression();

		return selectExpression instanceof ConstructorExpression;
	}

	@Override
	public DeclaredQuery getQuery() {
		return this.query;
	}

	/**
	 * Parses the query that is stored within the {@link EclipseLinkQueryEnhancer#query}. <br/>
	 * Notice: to optimize performance this function uses memorization.
	 *
	 * @return root expression of the parse query
	 */
	private Expression parseQuery() {

		if (this.memorizedRootExpression != null) {
			return this.memorizedRootExpression;
		}

		JPQLExpression jpqlExpression = new JPQLExpression(this.query.getQueryString(),
				DefaultEclipseLinkJPQLGrammar.instance(), false);

		Collection<JPQLQueryProblem> problems = new LinkedList<>();

		// Validate the JPQL query grammatically (based on the JPQL grammar)
		EclipseLinkGrammarValidator grammar = new EclipseLinkGrammarValidator(DefaultEclipseLinkJPQLGrammar.instance());
		grammar.setProblems(problems);
		jpqlExpression.accept(grammar);

		// at this state of query processing we do not have the
		// parameter which means we have to ignored parsing related errors
		problems = problems.stream()
				.filter(
						item -> !JPQLQueryProblemMessages.InputParameter_MissingParameter.equalsIgnoreCase(item.getMessageKey()))
				.toList();

		if (!problems.isEmpty()) {
			String problemsKeys = problems.stream().map(JPQLQueryProblem::getMessageKey).collect(Collectors.joining(","));
			throw new IllegalStateException(
					"The JPQL query you provided cannot be parse with EclipseLink since it is not valid JPQL! Detailed problem keys: "
							+ problemsKeys);
		}

		this.memorizedRootExpression = jpqlExpression.getQueryStatement();
		return this.memorizedRootExpression;
	}
}
