package dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.SellerDao;
import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.Department;
import model.Seller;

public class SellerDaoJDBC implements SellerDao{

	private Connection connection;
	
	public SellerDaoJDBC(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void insert(Seller obj) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement("INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES (?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, obj.getName());
			preparedStatement.setString(2, obj.getEmail());
			preparedStatement.setDate(3, new Date(obj.getBirthDate().getTime()));
			preparedStatement.setDouble(4, obj.getBaseSalary());
			preparedStatement.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = preparedStatement.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet resultSet = preparedStatement.getGeneratedKeys();
				if (resultSet.next()) {
					int id = resultSet.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(resultSet);
			} else {
				throw new DbException("Erro inesperado! Nenhuma linha afetada!");
			}
			
		} catch (SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		} finally {
			DB.closeStatement(preparedStatement);
		}
	}

	@Override
	public void update(Seller obj) {
		PreparedStatement preparedStatement = null;
		
		try {
			preparedStatement = connection.prepareStatement("UPDATE seller SET "
					+ "Name = ?, "
					+ "Email = ?, "
					+ "BirthDate = ?, "
					+ "BaseSalary = ?, "
					+ "DepartmentId = ? "
					+ "WHERE id = ?");
			preparedStatement.setString(1, obj.getName());
			preparedStatement.setString(2, obj.getEmail());
			preparedStatement.setDate(3, new Date(obj.getBirthDate().getTime()));
			preparedStatement.setDouble(4, obj.getBaseSalary());
			preparedStatement.setInt(5, obj.getDepartment().getId());
			preparedStatement.setInt(6, obj.getId());
			
			preparedStatement.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		} finally {
			DB.closeStatement(preparedStatement);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement preparedStatement = null;
		
		try {
			preparedStatement = connection.prepareStatement("DELETE FROM seller WHERE Id = ?");
			preparedStatement.setInt(1, id);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		} finally {
			DB.closeStatement(preparedStatement);
		}
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement = connection.prepareStatement("SELECT seller.*, department.Name as DepName "
					+ "FROM seller INNER JOIN department ON seller.DepartmentId = department.Id WHERE "
					+ "seller.Id = ?");
			preparedStatement.setInt(1, id);
			resultSet = preparedStatement.executeQuery();
			if(resultSet.next()) {
				Department department = instantiateDepartment(resultSet);
				Seller seller = instantiateSeller(resultSet, department);
				
				return seller;
			}
			return null;
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(preparedStatement);
			DB.closeResultSet(resultSet);
		}
	}
	
	@Override
	public List<Seller> findByDepartment(Department department){
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List<Seller> sellers = new ArrayList<>();
		try {
			preparedStatement = connection.prepareStatement("SELECT seller.*, department.Name as DepName"
					+ " FROM seller INNER JOIN department"
					+ " ON seller.DepartmentId = department.Id WHERE DepartmentId=?"
					+ " ORDER BY Name");
			preparedStatement.setInt(1, department.getId());
			
			resultSet = preparedStatement.executeQuery();
			
			Map<Integer, Department> map = new HashMap<>();
			
			while(resultSet.next()) {
				
				Department dep = map.get(resultSet.getInt("DepartmentId"));
				
				if( dep == null) {
					dep = instantiateDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = instantiateSeller(resultSet, dep);
				
				sellers.add(seller);
			}
			
			return sellers;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(preparedStatement);
		}
	}

	private Seller instantiateSeller(ResultSet resultSet, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(resultSet.getInt("Id"));
		seller.setName(resultSet.getString("Name"));
		seller.setEmail(resultSet.getString("Email"));
		seller.setBirthDate(resultSet.getDate("BirthDate"));
		seller.setBaseSalary(resultSet.getDouble("BaseSalary"));
		seller.setDepartment(department);
		
		return seller;
	}

	private Department instantiateDepartment(ResultSet resultSet) throws SQLException {
		Department department = new Department();
		department.setId(resultSet.getInt("DepartmentId"));
		department.setName(resultSet.getString("DepName"));
		return department;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		
		try {
			List<Seller> sellers = new ArrayList<>();
			preparedStatement = connection.prepareStatement("SELECT seller.*, department.Name as DepName"
					+ " FROM seller INNER JOIN department"
					+ " ON seller.DepartmentId = department.Id"
					+ " ORDER BY Name");
			resultSet = preparedStatement.executeQuery();
			
			Map<Integer, Department> map = new HashMap<>();
			
			while(resultSet.next()) {
				
				Department dep = map.get(resultSet.getInt("DepartmentId"));
				
				if( dep == null) {
					dep = instantiateDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = instantiateSeller(resultSet, dep);
				
				sellers.add(seller);
			}
			
			return sellers;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(preparedStatement);
		}
	}

}
